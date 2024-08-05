import Knex from 'knex';
import fs from 'fs/promises';
import moment from 'moment';
import parse from 'json-parse-safe';
import path from 'path';

const db = Knex ({ client: 'sqlite3', connection: { filename: 'data.db' }, useNullAsDefault: true });

function makeLedgersTable (table) {
  table.increments ('id').primary ();
  table.string ('name').unique ();
  table.enu ('default_currency', ['USD', 'EUR', 'CAD', 'PLN']);
}

function makeTransactionsTable (table) {
  table.increments ('id').primary ();
  table.string ('name');
  table.enu ('currency', ['USD', 'EUR', 'CAD', 'PLN']);
  table.string ('category');
  table.date ('date');
  table.float ('exchange_rate');
  table.string ('expense_type');
  table.boolean ('recurring');
  table.integer ('ledger_id').references ('id').inTable ('ledgers');
}

function makeMembersTable (table) {
  table.increments ('id').primary ();
  table.string ('name');
  table.integer ('ledger_id').references ('id').inTable ('ledgers');
  table.boolean ('active');
  table.unique (['name', 'ledger_id']);
}

function makeTransactionsMembersJunction (table) {
  table.increments ('id').primary ();
  table.integer ('transaction_id').references ('id').inTable ('transactions');
  table.integer ('member_id').references ('id').inTable ('members');
  table.integer ('ledger_id').references ('id').inTable ('ledgers');
  table.integer ('amount'); // integer in cents
  table.float ('weight');
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
  var ledger_id = await db ('ledgers').insert ({ name: ledgername, default_currency: 'CAD' }).returning ('id').then (rows => rows[0].id);
  var json = await fs.readFile (filename);
  var txs = parse (json).value;

  console.log (ledgername, txs);

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

  // insert members into the members table and build a map of member names to ids
  var memberToIdMap = await db ('members').insert (members.map (name => ({ name: name, ledger_id: ledger_id, active: true }))).returning ([ 'name', 'id' ])
    .then (rows => reduceToObject (rows, row => row.name, row => row.id));

  var txsMemberJunctionRows = [];

  // build the rows for the transactions_member_junction table (but use a two-dimensional array so we can add tx_id later)
  for (var tx of txs) {
    var transactionMemberPairs = reduceToObject (members, name => name, name => ({ member_id: memberToIdMap[name], amount: 0, weight: 0, ledger_id: ledger_id }));

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
    tx.ledger_id = ledger_id;

    if (tx.name === undefined || tx.name === null) tx.name = '';
    if (tx.category === undefined || tx.category === null) tx.category = '';

    if (tx.name === '' && tx.category === '') {
      tx.name = 'Unnamed transaction';
    }

    // delete extra keys from the transaction object
    delete tx.for;
    delete tx.by;
    delete tx.id;
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
