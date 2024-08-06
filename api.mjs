import Fastify from 'fastify';
import Knex from 'knex';
import _ from 'lodash';
import Decimal from 'decimal.js';

import seedrandom from 'seedrandom';
import moment from 'moment';
import got from 'got';
import { customAlphabet } from 'nanoid';

Decimal.set ({ rounding: Decimal.ROUND_HALF_EVEN });

const db = Knex ({ client: 'sqlite3', connection: { filename: 'data.db' }, useNullAsDefault: true });
const app = Fastify ({ logger: true });
const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
const nanoid = customAlphabet (alphabet, 10);

async function membersGetHandler (request, reply) {
  const filters = _.pick (request.query, ['ledger', 'name', 'active']);

  if (request.params.ledgerName) filters.ledger = request.params.ledgerName;
  if (request.params.memberName) filters.name = request.params.memberName;

  const isRequestingSingleMember = request.params.memberName !== undefined && request.params.ledgerName !== undefined;

  const members = await db ('members')
    .select ('members.name', 'members.ledger', 'members.active')
    .modify (builder => {
      if (filters.ledger) builder.where ('ledgers.name', filters.ledger);
      if (filters.name) builder.where ('members.name', filters.name);
      if (filters.active) builder.where ('members.active', filters.active);
    })
    .then (members => members.map (member => ({ name: member.name, ledger: member.ledger, active: Boolean (member.active) })));

  if (isRequestingSingleMember && members.length === 0) {
    return reply.code (404).send ({ error: 'The specified resource could not be found.' });
  }

  return isRequestingSingleMember ? members[0] : members;
}

async function membersPutHandler (request, reply) {
  const { memberName, ledgerName } = request.params;


  if (await db ('ledgers').where ({ name: ledgerName }).first () === undefined) {
    return reply.code (404).send ({ error: 'The specified ledger could not be found.' });
  }

  // request.body is either {} or { active: true/false }, if its empty make it true
  const member = { name: memberName, ledger: ledgerName, active: request.body.active === undefined ? true : request.body.active };

  try {
    await db.transaction (async trx => {
      const existingMember = await trx ('members').where ({ name: memberName, ledger: ledgerName }).first ();

      if (existingMember) {
        await trx ('members').where ({ name: memberName, ledger: ledgerName }).update (member);
      } else {
        await trx ('members').insert (member);
      }
    });

    reply.code (201).send ({ message: 'Member updated successfully.', member });
  } catch (error) {
    reply.code (500).send ({ error: `Internal server error: ${error.message}` });
  }
}

function shuffle (array, random) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(random () * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

function integerSplitByWeights (totalAmount, weights, seed) {
  if (seed === undefined) {
    seed = totalAmount;
  }

  const random = seedrandom (seed);
  const totalWeight = _.sum (weights);
  const rawShares = weights.map (weight => (weight / totalWeight) * totalAmount);

  // Initial rounding down of each share
  const flooredShares = rawShares.map (Math.floor);
  const flooredTotal = _.sum (flooredShares);

  // Calculate the remainder to distribute
  let remainder = totalAmount - flooredTotal;

  // Create a pseudo-random but deterministic order using the Fisher-Yates shuffle
  const result = [...flooredShares];
  const indices = weights.map ((_, index) => index);
  
  shuffle (indices, random);

  // If all the results are zero, just distribute the remainder normally
  if (flooredTotal === 0) {
    for (let i = 0; remainder > 0; i = (i + 1) % indices.length) {
      result[indices[i]]++;
      remainder--;
    }
  
  }
  // Otherwise, distribute the remainder only to the non-zero shares
  else {
    for (let i = 0; remainder > 0; i = (i + 1) % indices.length) {
      if (result[indices[i]] != 0) {
        result[indices[i]]++;
        remainder--;
      }

      if (result.every (share => share === 0)) {
        throw new Error ('Assertion failed: all shares are zero.');
      }
    }
  }

  return result;
}

function dollars (cents) {
  return new Decimal (cents).dividedBy (100).toNumber ();
}


async function getTransactions (filters, options = { format: 'array', useExchangeRates: false, moneyFormat: 'dollars' }) {
  if (options.moneyFormat !== 'dollars' && options.moneyFormat !== 'cents') {
    throw new Error ('Invalid money format.');
  }

  const query = db ('transactions as t')
    .join ('transactions_member_junction as tm', 't.id', 'tm.transaction_id')
    .select (
      't.id',
      't.name',
      't.currency',
      't.category',
      't.date',
      't.exchange_rate',
      't.expense_type',
      't.recurring',
      'tm.member',
      'tm.amount',
      'tm.weight',
      'tm.ledger'
    )
    .orderBy ([{ column: 't.date', order: 'desc' }, { column: 't.created_at', order: 'desc' }])
    .modify (builder => {
      if (filters.id) builder.where ('t.id', filters.id);
      if (filters.ledger) builder.where ('tm.ledger', filters.ledger);
      if (filters.name) builder.where ('t.name', filters.name);
      if (filters.category) builder.where ('t.category', filters.category);
      if (filters.currency) builder.where ('t.currency', filters.currency);
      if (filters.expense_type) builder.where ('t.expense_type', filters.expense_type);
      if (filters.recurring) builder.where ('t.recurring', filters.recurring);
      if (filters.dateAfter) builder.where ('t.date', '>=', filters.dateAfter);
      if (filters.dateBefore) builder.where ('t.date', '<=', filters.dateBefore);
    });

  const payload = await query;

  const uniqueIds = _.uniq (payload.map (transaction => transaction.id));
  const groupedTransactions = _.groupBy (payload, 'id');
  const transactionsRaw = uniqueIds.map (id => groupedTransactions[id]);

  const transactions = transactionsRaw.map (transactions => {
    let transaction = _.omit (transactions[0], ['member', 'amount', 'weight']);
    
    const multiplier = options.useExchangeRates ? transaction.exchange_rate : 1;
    const paid = transactions.map (t => new Decimal (t.amount).times (multiplier).round ().toNumber ());

    transaction.recurring = Boolean (transaction.recurring);

    // Basic array format structure
    transaction.total = _(paid).sum ();
    transaction.members = transactions.map (t => t.member);
    transaction.weights = transactions.map (t => t.weight);
    transaction.paid = paid;
    transaction.owes = integerSplitByWeights (transaction.total, transaction.weights, transactions[0]);

    if (options.moneyFormat === 'dollars') {
        transaction.paid = transaction.paid.map (dollars);
        transaction.owes = transaction.owes.map (dollars);
        transaction.total = dollars (transaction.total);
    }
 
    if (options.format === 'array') return transaction;

    if (options.format === 'object') {
      let member_contributions = transaction.members.map ((member, index) => ({ member, weight: transaction.weights[index], paid: transaction.paid[index], owes: transaction.owes[index] }));
      return _.omit ({ ...transaction, member_contributions }, ['members', 'weights', 'paid', 'owes']);
    }

    if (options.format === 'hash') {
      let member_contributions = _ (transaction.members).map ((member, index) => [ member, { weight: transaction.weights[index], paid: transaction.paid[index], owes: transaction.owes[index] }]).fromPairs ().value ();
      return _.omit ({ ...transaction, member_contributions }, ['members', 'weights', 'paid', 'owes']);
    }

    throw new Error ('Invalid format.');
  });

  return transactions;
}


async function transactionsGetHandler (request, reply) {
  const filters = request.params.id ? { id: request.params.id } : request.query;

  try {
    if (filters.date) {
      throw { status: 400, message: 'The "date" filter is not supported. Please use "dateAfter" and "dateBefore" instead.' };
    }

    const transactions = await getTransactions (filters, { format: 'array', useExchangeRates: false, moneyFormat: 'dollars' });

    if (request.params.id && transactions.length === 0) {
      throw { status: 404, message: 'Transaction not found.' };
    }

    reply.send (request.params.id ? transactions[0] : transactions);
  } catch (error) {
    // Handle errors
    error.status = error.status || 500;
    
    if (error.status === 500) { 
      if (error.message) error.message = `Internal server error: ${error.message}`;
      else error.message = 'Internal server error: please contact the maintainer.';
    } else if (error.message === undefined || error.message === null) {
      error.message = 'Something was wrong with this request but we don\'t know what. Contact the maintainer.';
    }

    return reply.code (error.status).send ({ message: error.message });
  }
}

async function categoriesGetHandler (request, reply) {
  const defaultCategories = [
    "🛒 Groceries",
    "🍽️ Food",
    "💡 Utilities",
    "🏡 Household",
    "🏠 Rent",
    "🛠️ Maintenance",
    "🛡️ Insurance",
    "🏥 Health",
    "🎬 Entertainment",
    "👗 Clothing",
    "📚 Subscriptions",
    "💸 Transfer",
    "📶 Internet",
    "🚿 Water",
    "🔥 Gas",
    "🚡 Transportation",
    "⚡ Hydro",
    "❓ Miscellaneous"
  ];

  const { ledgerName } = request.params;

  const ledger = await db ('ledgers').where ({ name: ledgerName }).first ();

  if (ledger === undefined) {
    return reply.code (404).send ({ error: 'The specified ledger does not exist.' });
  }

  const payload = await db ('transactions').select ('category').where ('ledger', ledgerName).distinct ();

  const categories = payload.map (transaction => transaction.category).filter (category => category !== "");
  const allCategories = _.uniq ([ ...defaultCategories, ...categories ]);

  return allCategories;
}

async function balancesGetHandler (request, reply, options = { moneyFormat: 'dollars' }) {
  if (options.moneyFormat !== 'dollars' && options.moneyFormat !== 'cents') {
    reply.code (500).send ({ error: 'Internal server error: Invalid money format in function balanceGetHandler' });
  }

  const ledgerName = request.params.ledgerName;
  const ledger = await db ('ledgers').where ({ name: ledgerName }).first ();

  if (ledger === undefined) {
    return reply.code (404).send ({ error: 'The specified ledger does not exist.' });
  }

  const transactions = await getTransactions ({ ledger: ledgerName }, { format: 'hash', useExchangeRates: true, moneyFormat: 'cents' });
  const members = await db ('members').where ({ ledger: ledgerName }).select ('name').then (members => members.map (member => member.name));

  return members.map (member => {
    const paid = _ (transactions).map (transaction => transaction.member_contributions[member] ? transaction.member_contributions[member].paid : 0).sum ();
    const owes = _ (transactions).map (transaction => transaction.member_contributions[member] ? transaction.member_contributions[member].owes : 0).sum ();

    if (options.moneyFormat === 'dollars') {
      return { name: member, paid: dollars (paid), owes: dollars (owes), balance: dollars (paid - owes) };
    } else {
      return { name: member, paid, owes, balance: paid - owes };
    }
  });
}

async function settlementsGetHandler (request, reply) {
  let balances = await balancesGetHandler (request, reply, { moneyFormat: 'cents' });

  balances = balances.filter (balance => balance.balance !== 0);
  balances.sort ((a, b) => b.balance - a.balance);

  let settlements = [];

  while (balances.length > 1) {
    let payee = balances[0];
    let payer = balances[balances.length - 1];

    let amount = Math.min (Math.abs (payee.balance), Math.abs (payer.balance));

    settlements.push ({ payer: payer.name, payee: payee.name, amount: amount });

    payee.balance -= amount;
    payer.balance += amount;

    if (payee.balance === 0) balances.shift ();
    if (payer.balance === 0) balances.pop ();
  }

  return settlements.map (settlement => { return { payer: settlement.payer, payee: settlement.payee, amount: dollars (settlement.amount) } });

  // TODO: Find subgroups of zero sum transactions and settle them separately to reduce the number of transactions.
}

async function transactionsPutPostHandler (request, reply) {
  try {
    if (request.params.id) {
      const previousTransaction = await db ('transactions').where ({ id: request.params.id }).first ();

      if (previousTransaction === undefined) {
        throw { status: 404, message: 'Transaction not found. This API does not support creating new transactions with PUT requests, please use POST instead.' };
      }
    }

    let transaction = request.body;
    
    if (transaction.name) transaction.name = transaction.name.trim ();
    if (transaction.category) transaction.category = transaction.category.trim ();
    if (transaction.members) transaction.members = transaction.members.map (member => member.trim ());
    
    if (transaction.name === "") delete transaction.name;
    if (transaction.category === "") delete transaction.category;

    transaction.date = transaction.date || moment ().format ('YYYY-MM-DD');

    if (transaction.expense_type === 'income') {
      transaction.paid = transaction.paid.map (amount => -amount);
    }

    transaction.recurring = false; // reserved for future use

    await validateTransaction (transaction);

    const newTransaction = _.pick (transaction, ['name', 'currency', 'category', 'date', 'expense_type', 'recurring', 'ledger']);

    if (request.params.id === undefined) {
      try {
        const exchangeRates = await got ('https://open.er-api.com/v6/latest/CAD').json ();
        newTransaction.exchange_rate = 1 / exchangeRates.rates[transaction.currency];
      } catch (error) {
        throw { status: 500, message: 'Internal server error: Unable to get exchange rates.' };
      }

      newTransaction.created_at = moment ().format ('YYYY-MM-DD HH:mm:ss');
    }
   
    try {
      await db.transaction (async trx => {
        if (request.params.id) {
          transaction.id = request.params.id;
          
          await trx ('transactions').where ('id', transaction.id).update (newTransaction);
          await trx ('transactions_member_junction').where ('transaction_id', transaction.id).del ();
        } else {
          transaction.id = nanoid ();
          newTransaction.id = transaction.id;

          await trx ('transactions').insert (newTransaction);
        }

        const transactionsMemberJunctionItems = transaction.members.map ((member, index) => ({
          transaction_id: transaction.id,
          member: member,
          weight: transaction.weights[index],
          amount: Math.floor (transaction.paid[index] * 100),
          ledger: transaction.ledger
        }));
        
        await trx ('transactions_member_junction').insert (transactionsMemberJunctionItems);
      });
    } catch (error) {
      throw { status: 500, message: 'Internal server error: Unable to insert transaction into the database.' };
    }

    // Fetch and return the newly created transaction
    const [newTransactionWithId] = await getTransactions ({ id: transaction.id });
   
    const status = request.params.id ? 200 : 201;
    const message = `Transaction ${request.params.id ? 'updated' : 'created'} successfully.`;

    return reply.code (status).send ({ message, transaction: newTransactionWithId });
  } catch (error) {
    // Handle errors
    const status = error.status || 500;
    const message = error.message || 'Internal server error';
    return reply.code (status).send ({ error: message });
  }
}

async function validateTransaction (transaction) {
  // Validate and clean transaction data
  const supportedCurrencies = ['CAD', 'USD', 'EUR', 'PLN'];
  if (!supportedCurrencies.includes (transaction.currency)) {
    throw { status: 400, message: `Currency is not supported, we support the following currencies: ${supportedCurrencies.join (', ')}` };
  }
  
  if (transaction.weights.every (weight => weight === 0)) {
    throw { status: 400, message: 'All the weights are zero, the transaction is invalid.' };
  }
  
  if (transaction.members.length !== transaction.weights.length || transaction.members.length !== transaction.paid.length) {
    throw { status: 400, message: 'The number of members, weights, and paid amounts must be the same.' };
  }
  
  if (_.uniq (transaction.members).length !== transaction.members.length) {
    throw { status: 400, message: 'Members must be unique.' };
  }
  
  const ledger = await db ('ledgers').where ('name', transaction.ledger).first ();
  if (!ledger) throw { status: 400, message: 'The specified ledger does not exist.' };

  if (!transaction.name && !transaction.category) {
    throw { status: 400, message: 'A transaction must have a name or a category.' };
  }

  // Get the members of the ledger
  const members = await db ('members')
    .where ('ledger', transaction.ledger)
    .select ('name')
    .then (members => members.map (member => member.name));

  if (transaction.members.every (member => members.includes (member)) == false) {
    throw { status: 400, message: 'One or more members do not exist in the ledger.' };
  }
}

async function ledgersDeleteHandler (request, reply) {
  const ledgerName = request.params.ledgerName;

  // Start a transaction
  await db.transaction (async trx => {
    const ledger = await trx ('ledgers').where ('name', ledgerName).first ();

    if (ledger === undefined) {
      reply.code (204).send ({ message: 'The specified resource does not exist.' });
      return;
    }

    // Delete all members and transactions associated with the ledger
    await trx ('members').where ('ledger', ledgerName).del ();
    await trx ('transactions').where ('ledger', ledgerName).del ();
    await trx ('transactions_member_junction').where ('ledger', ledgerName).del ();
    await trx ('ledgers').where ('name', ledgerName).del ();

    reply.code (200).send ({ message: 'Ledger deleted successfully.' });
  }).catch (error => {
    console.error (error);
    reply.code (500).send ({ error: 'Internal server error: Unable to delete ledger.' });
  });
}

async function transactionsDeleteHandler (request, reply) {
  const id = request.params.id;

  await db.transaction (async trx => {
    const transaction = await trx ('transactions').where ('id', id).first ();

    if (transaction === undefined) {
      reply.code (204).send ({ message: 'The specified resource does not exist.' });
      return;
    }

    // Delete the transaction
    await trx ('transactions').where ('id', id).del ();

    // Delete the junction entries
    await trx ('transactions_member_junction').where ('transaction_id', id).del ();

    reply.code (200).send ({ message: 'Transaction deleted successfully.' });
  });
}

const ledgersGetHandler = async (request, reply) => { return await db ('ledgers').select (); }

async function ledgersGetHandlerWithRoute (request, reply) { 
  const payload = await db ('ledgers').where ({ name: request.params.ledgerName }).first ();

  if (payload === undefined) {
    return reply.code (404).send ({ error: 'The specified resource does not exist.' });
  }

  return payload;
}

async function ledgersPutHandler (request, reply) {
  request.body.name = request.params.ledgerName;

  const ledger = await db ('ledgers').where ('name', request.params.ledgerName).first ();

  if (ledger === undefined) {
    await db ('ledgers').insert (_.pick (request.body, ['name', 'currency']));
    return reply.code (201).send ({ message: 'Ledger created successfully.' });
  }

  await db ('ledgers').where ('name', request.params.ledgerName).update (_.pick (request.body, ['currency']));
  return reply.code (200).send ({ message: 'Ledger updated successfully.' });
}

const transactionPostBodySchema = {
  type: 'object',
  required: ['ledger', 'currency', 'expense_type', 'members', 'weights', 'paid'],
  anyOf: [
    { required: ['name'] },
    { required: ['category'] }
  ],
  properties: {
    ledger: { type: 'string' },
    currency: { type: 'string', enum: ['CAD', 'USD', 'EUR', 'PLN'] },
    expense_type: { type: 'string' },
    members: { 
      type: 'array',
      items: { type: 'string' },
      minItems: 1
    },
    weights: { 
      type: 'array',
      items: { type: 'number' },
      minItems: 1
    },
    paid: { 
      type: 'array',
      items: { type: 'number' },
      minItems: 1
    },
    name: { type: 'string' },
    category: { type: 'string' },
    date: { type: 'string', format: 'date' },
    recurring: { type: 'boolean' }
  },
  additionalProperties: false
};

const ledgersPutBodySchema = {
  type: 'object',
  required: ['currency'],
  properties: {
    currency: { type: 'string', enum: ['CAD', 'USD', 'EUR', 'PLN'] }
  },
  additionalProperties: false
};

const transactionsGetQuerySchema = {
  type: 'object',
  properties: {
    id: { type: 'integer' },
    ledger: { type: 'string' },
    name: { type: 'string' },
    category: { type: 'string' },
    currency: { type: 'string', enum: ['CAD', 'USD', 'EUR', 'PLN'] },
    date: { type: 'string', format: 'date' },
    expense_type: { type: 'string' },
    recurring: { type: 'boolean' },
    dateAfter: { type: 'string', format: 'date' },
    dateBefore: { type: 'string', format: 'date' }
  },
  additionalProperties: false
};

const membersGetResponseSchemaWithRoute = {
  type: 'object',
  required: ['name', 'ledger', 'active'],
  properties: {
    name: { type: 'string' },
    ledger: { type: 'string' },
    active: { type: 'boolean'}
  }
}

const membersGetResponseSchema = {
  type: 'array',
  items: membersGetResponseSchemaWithRoute
}

app.get ('/ledgers', ledgersGetHandler);
app.get ('/ledgers/:ledgerName', ledgersGetHandlerWithRoute);
app.put ('/ledgers/:ledgerName', { schema: { body: ledgersPutBodySchema } }, ledgersPutHandler);
app.delete ('/ledgers/:ledgerName', ledgersDeleteHandler);

app.get ('/members', { schema: { response: { 200: membersGetResponseSchema } } }, membersGetHandler);
app.get ('/members/:ledgerName/:memberName', { schema: { response: { 200: membersGetResponseSchemaWithRoute } } }, membersGetHandler);
app.put ('/members/:ledgerName/:memberName', membersPutHandler);

app.get ('/transactions', { schema: { querystring: transactionsGetQuerySchema } }, transactionsGetHandler);
app.get ('/transactions/:id', transactionsGetHandler);
app.post ('/transactions', { schema: { body: transactionPostBodySchema } }, transactionsPutPostHandler);
app.delete ('/transactions/:id', transactionsDeleteHandler);
app.put ('/transactions/:id', { schema: { body: transactionPostBodySchema } }, transactionsPutPostHandler);

app.get ('/ledgers/:ledgerName/categories', categoriesGetHandler);
app.get ('/ledgers/:ledgerName/balance', balancesGetHandler);
app.get ('/ledgers/:ledgerName/settlement', settlementsGetHandler);

try {
  await app.listen({ port: 3000 })
} catch (err) {
  app.log.error(err)
  process.exit(1)
}

