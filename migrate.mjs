import Knex from 'knex';
import fs from 'fs/promises';
import moment from 'moment';
import parse from 'json-parse-safe';
import path from 'path';
import { customAlphabet } from 'nanoid';

const db = Knex ({ client: 'sqlite3', connection: { filename: 'data.db' }, useNullAsDefault: true });
const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
const nanoid = customAlphabet (alphabet, 10);

function makeLedgersTable (table) {
  table.string ('name').primary ();
  table.enu ('default_currency', ['USD', 'EUR', 'CAD', 'PLN']);
}

function makeTransactionsTable (table) {
  table.string ('id').primary ();
  table.string ('name');
  table.enu ('currency', ['USD', 'EUR', 'CAD', 'PLN']);
  table.string ('category');
  table.date ('date');
  table.float ('exchange_rate');
  table.string ('expense_type');
  table.boolean ('recurring');
  table.string ('ledger').references ('name').inTable ('ledgers');
}

function makeMembersTable (table) {
  table.string ('name');
  table.string ('ledger').references ('name').inTable ('ledgers');
  table.boolean ('active');
  table.primary (['name', 'ledger']);
}

function makeTransactionsMembersJunction (table) {
  table.string ('transaction_id').references ('id').inTable ('transactions');
  table.string ('member');
  table.string ('ledger').references ('name').inTable ('ledgers');
  table.integer ('amount'); // integer in cents
  table.float ('weight');
  table.foreign (['member', 'ledger']).references (['name', 'ledger']).inTable ('members');
  table.primary (['transaction_id', 'member', 'ledger']);
}

async function createTables () {
  await db.schema.createTable ('ledgers', makeLedgersTable);
  await db.schema.createTable ('transactions', makeTransactionsTable);
  await db.schema.createTable ('members', makeMembersTable);
  await db.schema.createTable ('transactions_member_junction', makeTransactionsMembersJunction);
}

function reduceToObject (array, keyFunction, valueFunction) {
  return array.reduce ((accumulator, current) => {
    accumulator[keyFunction (current)] = valueFunction (current);
    return accumulator;
  }, {});
}

async function importTransactionsFromJsonFile (filename, ledgername) {
  await db ('ledgers').insert ({ name: ledgername, default_currency: 'CAD' });

  var json = await fs.readFile (filename);
  var txs = parse (json).value;

  // return if txs is empty
  if (!txs) return;

  // if txs is just an object, put it in an array
  if (!Array.isArray (txs)) txs = [txs];

  // strip whitespace from fields
  for (var tx of txs) {
    if (tx.name) tx.name = tx.name.trim ();
    if (tx.category) tx.category = tx.category.trim ();

    tx.by.members = tx.by.members.map (member => member.trim ());
    tx.for.members = tx.for.members.map (member => member.trim ());
  }
  

  // get all unique members in the transactions
  var members = [...new Set (txs.map (tx => [...tx.for.members, ...tx.by.members]).flat ())];

  await db ('members').insert (members.map (name => ({ name: name, ledger: ledgername, active: true })));

  var txsMemberJunctionRows = [];

  // build the rows for the transactions_member_junction table (but use a two-dimensional array so we can add tx_id later)
  for (var tx of txs) {
    var transactionMemberPairs = reduceToObject (members, name => name, name => ({ member: name, amount: 0, weight: 0, ledger: ledgername }));

    tx.for.members.forEach ((name, index) => transactionMemberPairs[name].weight = tx.for.split_weights[index]);
    tx.by.members.forEach ((name, index) => transactionMemberPairs[name].amount = Math.floor (tx.by.split_values[index] * 100)); // convert to cents

    transactionMemberPairs = Object.values (transactionMemberPairs);

    // remove members with zero amount and weight
    transactionMemberPairs = transactionMemberPairs.filter (pair => pair.amount !== 0 || pair.weight !== 0);

    txsMemberJunctionRows.push (Object.values (transactionMemberPairs));
  }

  // clean up the rows for the transactions table
  for (var tx of txs) {
    // change date format from epoch to ISO
    tx.date = moment.unix (Math.floor(tx.date / 1000)).format ('YYYY-MM-DD');
    
    tx.recurring = false; // reserve for future use
    tx.ledger = ledgername;

    if (tx.name === undefined || tx.name === null) tx.name = '';
    if (tx.category === undefined || tx.category === null) tx.category = '';

    if (tx.name === '' && tx.category === '') {
      tx.name = 'Unnamed transaction';
    }

    tx.id = nanoid ();

    // delete extra keys from the transaction object
    delete tx.for;
    delete tx.by;
    delete tx.converted_total;
  }

  var ids = [];

  for (var i = 0; i < txs.length; i += 500) {
    var txChunk = txs.slice (i, i + 500);
    var txChunkIds = await db ('transactions').insert (txChunk).returning ('id');
    ids.push (...txChunkIds);
  }

  ids.forEach ((id, index) => txsMemberJunctionRows[index].forEach (row => row.transaction_id = id.id));
  txsMemberJunctionRows = txsMemberJunctionRows.flat ();

  // insert rows into the transactions_members join table in 500 row chunks
  for (var i = 0; i < txsMemberJunctionRows.length; i += 500) {
    await db ('transactions_member_junction').insert (txsMemberJunctionRows.slice (i, i + 500));
  }

}

await createTables ();

const filenames = process.argv.slice (2).map (filename => path.parse (filename));

for (var filename of filenames) {
  const fullpath = path.join (filename.dir, filename.base);
  await importTransactionsFromJsonFile (fullpath, filename.name);
}

process.exit (0);
