import Fastify from 'fastify';
import Knex from 'knex';
import RRuleModule from 'rrule'; const { RRule, datetime } = RRuleModule;
import { sum, uniq, omit, pick, groupBy, fromPairs } from 'lodash-es';
import cors from '@fastify/cors';
import cron from 'node-cron';

import { getDateString, getDateTimeString, generateId, integerSplitByWeights, integerMultiplyByFloat, integerCentsToDollars } from './utilities.js';

const db = Knex ({ client: 'sqlite3', connection: { filename: 'data.db' }, useNullAsDefault: true });
const app = Fastify ({ logger: true });

app.register (cors, { origin: '*' });

async function membersGetHandler (request, reply) {
  const filters = pick (request.query, ['ledger', 'name', 'active']);

  if (request.params.ledgerName) filters.ledger = request.params.ledgerName;
  if (request.params.memberName) filters.name = request.params.memberName;

  const isRequestingSingleMember = request.params.memberName !== undefined && request.params.ledgerName !== undefined;

  const members = await db ('members')
    .select ('name', 'ledger', 'active')
    .modify (builder => {
      if (filters.ledger) builder.where ('ledger', filters.ledger);
      if (filters.name) builder.where ('name', filters.name);
      if (filters.active) builder.where ('active', filters.active);
    })
    .then (members => members.map (member => ({ name: member.name, ledger: member.ledger, active: Boolean (member.active) })));

  if (isRequestingSingleMember && members.length === 0) {
    return reply.code (404).send ({ error: 'The specified resource could not be found.' });
  }

  return isRequestingSingleMember ? members[0] : members;
}

async function membersPutHandler (request, reply) {
  const { memberName, ledgerName } = request.params;

  const resolvedLedgerName = await db ('ledgers').where ({ name: ledgerName }).first ().then (ledger => ledger ? ledger.name : undefined);

  if (resolvedLedgerName === undefined) {
    return reply.code (404).send ({ error: 'The specified resource could not be found.' });
  }

  // request.body is either {} or { active: true/false }, if its empty make it true
  const member = { name: memberName, ledger: ledgerName, active: request.body.active === undefined ? true : request.body.active };

  // if request.body.name is present, and it matches the memberName (up to case), use it
  if (request.body.name) {
    if (request.body.name.toLowerCase () === memberName.toLowerCase ()) {
      member.name = request.body.name;
    } else {
      // this is a malformed request, the memberName in the path does not match the member name in the object
      return reply.code (400).send ({ error: 'The member name in the path does not match the member name in the object.' });
    }
  }

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


async function getTransactions (filters, options = { format: 'array', useExchangeRates: false, moneyFormat: 'dollars' }, trx = db) {
  if (options.moneyFormat !== 'dollars' && options.moneyFormat !== 'cents') {
    throw new Error ('Invalid money format.');
  }

  const query = trx ('transactions as t')
    .join ('transactions_member_junction as tm', 't.id', 'tm.transaction_id')
    .select (
      't.id',
      't.name',
      't.currency',
      't.category',
      't.date',
      't.exchange_rate',
      't.expense_type',
      'tm.member',
      'tm.amount',
      'tm.weight',
      'tm.ledger'
    )
    .orderBy ([{ column: 't.date', order: 'desc' }, { column: 't.created_at', order: 'desc' }])
    .modify (builder => {
      builder.where ('t.is_template', false);
      builder.where ('t.is_deleted', false);

      if (filters.id) builder.where ('t.id', filters.id);
      if (filters.ledger) builder.where ('tm.ledger', filters.ledger);
      if (filters.name) builder.where ('t.name', filters.name);
      if (filters.category) builder.where ('t.category', filters.category);
      if (filters.currency) builder.where ('t.currency', filters.currency);
      if (filters.expensetype) builder.where ('t.expense_type', filters.expensetype);
      if (filters.dateafter) builder.where ('t.date', '>=', filters.dateafter);
      if (filters.datebefore) builder.where ('t.date', '<=', filters.datebefore);
    });

  const payload = await query;

  const uniqueIds = uniq (payload.map (transaction => transaction.id));
  const groupedTransactions = groupBy (payload, 'id');
  const transactionsRaw = uniqueIds.map (id => groupedTransactions[id]);

  const transactions = transactionsRaw.map (transactions => {
    let transaction = omit (transactions[0], ['member', 'amount', 'weight']);
    
    const multiplier = options.useExchangeRates ? transaction.exchange_rate : 1;
    const paid = transactions.map (({amount}) => integerMultiplyByFloat (amount, multiplier));

    // Basic array format structure
    transaction.amount = sum (paid);
    transaction.members = transactions.map (t => t.member);
    transaction.weights = transactions.map (t => t.weight);
    transaction.paid = paid;
    transaction.owes = integerSplitByWeights (transaction.amount, transaction.weights, transactions[0]);

    if (options.moneyFormat === 'dollars') {
        transaction.paid = transaction.paid.map (integerCentsToDollars);
        transaction.owes = transaction.owes.map (integerCentsToDollars);
        transaction.amount = integerCentsToDollars (transaction.amount);
    }
 
    if (options.format === 'array') return transaction;

    if (options.format === 'object') {
      const memberContributions = transaction.members.map ((m, i) => ({ member: m, weight: transaction.weights[i], paid: transaction.paid[i], owes: transaction.owes[i] }));
      return omit ({ ...transaction, contributions: memberContributions }, ['members', 'weights', 'paid', 'owes']);
    }

    if (options.format === 'hash') {
      const memberContributions = transaction.members.map ((m, i) => [ m, { weight: transaction.weights[i], paid: transaction.paid[i], owes: transaction.owes[i] }]);

      return omit ({ ...transaction, contributions: fromPairs (memberContributions) }, ['members', 'weights', 'paid', 'owes']);
    }

    throw new Error ('Invalid format.');
  });

  return transactions;
}


async function transactionsGetHandler (request, reply) {
  const filters = request.params.id ? { id: request.params.id } : request.query;

  try {
    if (filters.date) {
      throw { status: 400, message: 'The "date" filter is not supported. Please use "dateafter" and "datebefore" instead.' };
    }

    const transactions = await getTransactions (filters, { format: 'object', useExchangeRates: false, moneyFormat: 'dollars' });

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

  const payload = await db ('transactions').select ('category')
    .where ('ledger', ledgerName)
    .where ('is_template', false)
    .where ('is_deleted', false)
    .distinct ();

  const categories = payload.map (transaction => transaction.category).filter (category => category !== "");
  const allCategories = uniq ([ ...defaultCategories, ...categories ]);

  return allCategories;
}


async function getBalance (ledger, options = { moneyFormat: 'dollars' }, trx = db) {
  if (options.moneyFormat !== 'dollars' && options.moneyFormat !== 'cents') {
    throw new Error ('Invalid money format. Please contact the maintainer.');
  }

  const ledgerExists = await trx ('ledgers').where ({ name: ledger }).first ();

  if (ledgerExists === undefined) return undefined;

  const transactions = await getTransactions ({ ledger }, { format: 'hash', useExchangeRates: true, moneyFormat: 'cents' }, trx);
  const members = await trx ('members').where ({ ledger }).select ('name', 'active');

  const processMember = (member) => {
    const m = member.name;

    const paid = sum (transactions.map (transaction => transaction.contributions[m] ? (transaction.expense_type === 'income' ? -transaction.contributions[m].paid : transaction.contributions[m].paid) : 0));
    const owes = sum (transactions.map (transaction => transaction.contributions[m] ? (transaction.expense_type === 'income' ? -transaction.contributions[m].owes : transaction.contributions[m].owes) : 0));
    const balance = paid - owes;

    if (member.active !== true) {
      if (balance !== 0) throw new Error ('Assertion failure: inactive member has a non-zero balance. Please contact the maintainer.');
      return undefined;
    }

    if (options.moneyFormat === 'dollars') {
      return { name: m, paid: integerCentsToDollars (paid), owes: integerCentsToDollars (owes), balance: integerCentsToDollars (balance) };
    } else {
      return { name: m, paid, owes, balance };
    }
  }

  return members.map (processMember).filter (m => m !== undefined);
}

async function balancesGetHandler (request, reply) {
  try {
    const balance = await getBalance (request.params.ledgerName);

    if (balance === undefined) {
      return reply.code (404).send ({ error: 'The specified ledger does not exist.' });
    }

    return balance;
  } catch (error) {
    return reply.code (500).send ({ error: `Internal server error: ${error.message}` });
  }
}

async function settlementsGetHandler (request, reply) {
  let balances = await getBalance (request.params.ledgerName, { moneyFormat: 'cents' });

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

  return settlements.map (settlement => { return { payer: settlement.payer, payee: settlement.payee, amount: integerCentsToDollars (settlement.amount) } });

  // TODO: Find subgroups of zero sum transactions and settle them separately to reduce the number of transactions.
}

async function updateAddTransaction (transaction, isUpdate) {
  if (transaction.name) transaction.name = transaction.name.trim ();
  if (transaction.category) transaction.category = transaction.category.trim ();
  if (transaction.members) transaction.members = transaction.members.map (member => member.trim ());
  
  if (transaction.name === "") transaction = omit (transaction, 'name');
  if (transaction.category === "") transaction = omit (transaction, 'category');

  transaction.date = transaction.date || getDateString ();

  await validateTransaction (transaction);

  // if (transaction.expense_type === 'income') {
  //  transaction.paid = transaction.paid.map (amount => -amount);
  // }

  transaction.is_deleted = false;

  const newTransaction = pick (transaction, ['name', 'currency', 'category', 'date', 'expense_type', 'ledger', 'is_template', 'is_deleted']);

  if (!isUpdate) {
    try {
      if (transaction.currency === 'CAD') {
        newTransaction.exchange_rate = 1;
      } else {
        const exchangeRates = await fetch ('https://open.er-api.com/v6/latest/CAD').then (response => response.json ());
        newTransaction.exchange_rate = 1 / exchangeRates.rates[transaction.currency];
      }
    } catch (error) {
      throw new Error ('Internal server error: Unable to get exchange rates.');
    }

    newTransaction.created_at = getDateTimeString ();
  }
  
  try {
    await db.transaction (async trx => {
      if (isUpdate) {
        const is_deleted = await trx ('transactions').where ({ id: transaction.id }).select ('is_deleted').first ();

        if (is_deleted === undefined) {
          throw new Error ('The specified transaction does not exist.');
        }

        if (is_deleted.is_deleted === true) {
          throw new Error ('The specified transaction has been deleted.');
        }

        await trx ('transactions').where ('id', transaction.id).update (newTransaction);
        await trx ('transactions_member_junction').where ('transaction_id', transaction.id).del ();
      } else {
        transaction.id = generateId ();
        newTransaction.id = transaction.id;

        await trx ('transactions').insert (newTransaction);
      }

      const transactionsMemberJunctionItems = transaction.members.map ((member, index) => ({
        transaction_id: transaction.id,
        member: member,
        weight: transaction.weights[index],
        amount: Math.floor (transaction.paid[index] * 100),
        ledger: transaction.ledger
      })).filter (item => item.amount !== 0 || item.weight !== 0);
      
      await trx ('transactions_member_junction').insert (transactionsMemberJunctionItems);
    });
  } catch (error) {
    throw new Error ('Internal server error: Unable to insert transaction into the database.');
  }

  return transaction.id;
}

async function transactionsPutPostHandler (request, reply) {
  try {
    if (request.params.id) {
      const previousTransaction = await db ('transactions').where ({ id: request.params.id }).first ();

      if (previousTransaction === undefined) {
        throw { status: 404, message: 'Transaction not found. This API does not support creating new transactions with PUT requests, please use POST instead.' };
      }

      request.body.id = request.params.id;
    }
   
    request.body.is_template = false;

    const id = await updateAddTransaction (request.body, request.params.id !== undefined);

    // Fetch and return the newly created transaction
    const [newTransactionWithId] = await getTransactions ({ id });
   
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
  
  if (uniq (transaction.members).length !== transaction.members.length) {
    throw { status: 400, message: 'Members must be unique.' };
  }
  
  const ledger = await db ('ledgers').where ('name', transaction.ledger).first ();
  if (!ledger) throw { status: 400, message: 'The specified ledger does not exist.' };

  if (!transaction.name && !transaction.category) {
    throw { status: 400, message: 'A transaction must have a name or a category.' };
  }

  if (transaction.expense_type !== 'expense' && transaction.expense_type !== 'income' && transaction.expense_type !== 'transfer') {
    throw { status: 400, message: 'The expense type must be either "expense", "income", or "transfer".' };
  }

  // All the transaction amounts should be positive
  if (transaction.paid.some (amount => amount <= 0)) {
    throw { status: 400, message: `All the ${transaction.expense_type === 'income' ? 'received' : 'paid'} amounts must be positive.` };
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
    await trx ('recurrences').join ('transactions', 'recurrences.template_id', 'transactions.id').where ('transactions.ledger', ledgerName).del ();
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

    if (transaction.is_template) {
      reply.code (400).send ({ message: 'Templates cannot be deleted.' });
      return;
    }

    // Change transaction.is_deleted to true
    await trx ('transactions').where ('id', id).update ('is_deleted', true);

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
    await db ('ledgers').insert (pick (request.body, ['name', 'currency']));
    return reply.code (201).send ({ message: 'Ledger created successfully.' });
  }

  await db ('ledgers').where ('name', request.params.ledgerName).update (pick (request.body, ['currency']));
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
    id: { type: 'string' },
    ledger: { type: 'string' },
    name: { type: 'string' },
    category: { type: 'string' },
    currency: { type: 'string', enum: ['CAD', 'USD', 'EUR', 'PLN'] },
    date: { type: 'string', format: 'date' },
    expensetype: { type: 'string' },
    dateafter: { type: 'string', format: 'date' },
    datebefore: { type: 'string', format: 'date' }
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

async function createRecurringTransactions () {
  const recurrences = await db ('recurrences')
    .join ('transactions', 'recurrences.template_id', 'transactions.id')
    .leftJoin ('transactions as last_transaction', 'recurrences.last_created_id', 'last_transaction.id')
    .select ('recurrences.*', 'transactions.*', 'last_transaction.date as last_date');

  for (const recurrence of recurrences) {
    const rule = RRule.fromString (recurrence.rrule);

    const baseDate = new Date (recurrence.last_date || recurrence.date);
    const lastDate = new Date (baseDate);
    lastDate.setDate (baseDate.getDate () + 1);

    const now = new Date ();
    const dates = rule.between (lastDate, now);

    if (dates.length === 0) {
      continue;
    }

    const transactions = dates.map (date => {
      const transaction = pick (recurrence, [
        'name',
        'currency',
        'category',
        'expense_type',
        'ledger',
      ]);

      transaction.id = generateId ();
      transaction.created_at = getDateTimeString ();
      transaction.date = getDateString (date);
      transaction.exchange_rate = 1; // TODO: implement exchange rates
      transaction.is_template = false;
      transaction.is_deleted = false;

      return transaction;
    });

    // Perform database operations in a transaction
    try {
      await db.transaction (async trx => {
        const ids = await trx ('transactions')
          .insert (transactions)
          .returning ('id');

        const transactionMemberJunctions = await trx ('transaction_member_junction')
          .where ('transaction_id', recurrence.template_id)
          .select ();

        const newTransactionMemberJunctions = ids
          .map (id =>
            transactionMemberJunctions.map (junction => ({
              transaction_id: id,
              member: junction.member,
              weight: junction.weight,
              amount: junction.amount,
              ledger: junction.ledger,
            }))
          )
          .flat ();

        await trx ('transaction_member_junction').insert (newTransactionMemberJunctions);

        // Update last_created_id
        const lastCreatedId = ids[ids.length - 1];
        await trx ('recurrences')
          .where ('id', recurrence.id)
          .update ({ last_created_id: lastCreatedId });
      });
    } catch (error) {
      console.error (`Failed to process recurrence - ${recurrence.id}:`, error);
      // Optionally, handle the error or rethrow
    }
  }
}

// cron.schedule ('0 0 * * *', createRecurringTransactions);

app.get ('/ledgers', ledgersGetHandler);
app.get ('/ledgers/:ledgerName', ledgersGetHandlerWithRoute);
app.put ('/ledgers/:ledgerName', { schema: { body: ledgersPutBodySchema } }, ledgersPutHandler);
// app.delete ('/ledgers/:ledgerName', ledgersDeleteHandler);

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

// app.get ('/recurrences', recurrencesGetHandler);
// app.get ('/recurrences/:id', recurrencesGetHandler);
// app.post ('/recurrences', recurrencesPutPostHandler);
// app.delete ('/recurrences/:id', recurrencesDeleteHandler);
// app.put ('/recurrences/:id', recurrencesPutPostHandler);


const start = async () => {
  try {
    await app.listen ({ port: 3001 })
  } catch (error) {
    app.log.error (error)
    process.exit (1)
  }
}

start ();
