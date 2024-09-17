async function fetchjson (url) {
  try {
    return await fetch (url).then (response => response.json ());
  } catch (error) {
    console.error (error)
  }
}

export async function xpnz (base) {
  async function loadLedgers () {
    return await fetchjson (`${base}/ledgers`);
  }

  async function loadLedgerCategories (name) {
    return await fetchjson (`${base}/ledgers/${name}/categories`);
  }

  async function loadLedgerBalance (name) {
    return await fetchjson (`${base}/ledgers/${name}/balance`);
  }

  async function loadLedgerSettlement (name) {
    return await fetchjson (`${base}/ledgers/${name}/settlement`);
  }

  async function loadMembers () {
    return await fetchjson (`${base}/members`);
  }

  async function loadMembersByLedger (name) {
    return await fetchjson (`${base}/members?ledger=${name}`);
  }

  async function loadTransaction (id) {
    return await fetchjson (`${base}/transactions/${id}`);
  }

  async function loadTransactions () {
    return await fetchjson (`${base}/transactions`);
  }

  async function loadTransactionsByLedger (name) {
    return await fetchjson (`${base}/transactions?ledger=${name}`);
  }

  async function loadCompleteLedger (name) {
    const [members, transactions, categories, balance, settlement] = await Promise.all ([
      loadMembersByLedger (name),
      loadTransactionsByLedger (name),
      loadLedgerCategories (name),
      loadLedgerBalance (name),
      loadLedgerSettlement (name)
    ])

    return { ledger: name, members, transactions, categories, balance, settlement }
  }


  async function newTransaction (name, category, ledger, currency, expense_type, members, weights, paid) {
    const transaction = {
      name, category, ledger, currency, expense_type, members, weights, paid
    };

    try {
      return await fetch (`${base}/transactions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify (transaction)
      }).then (data => data.json ())
    } catch (error) {
      console.error (error)
    }
  }

  async function editTransaction (id, name, category, ledger, currency, expense_type, members, weights, paid) {
    const transaction = {
      name, category, ledger, currency, expense_type, members, weights, paid
    };

    try {
      return await fetch (`${base}/transactions/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify (transaction)
      }).then (data => data.json ())
    } catch (error) {
      console.error (error)
    }
  }

  async function newMember (name, ledger) {
    try {
      return await fetch (`${base}/members/${ledger}/${name}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify ({}),
      }).then (data => data.json ())
    } catch (error) {
      console.error (error)
    }
  }

  async function newLedger (ledger, currency) {
    try {
      return await fetch (`${base}/ledgers/${ledger}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify ({currency}),
      }).then (data => data.json ())
    } catch (error) {
      console.error (error)
    }
  }

  return {
    loadLedgers,
    loadLedgerCategories,
    loadLedgerBalance,
    loadLedgerSettlement,
    loadMembers,
    loadMembersByLedger,
    loadTransaction,
    loadTransactions,
    loadTransactionsByLedger,
    loadCompleteLedger,
    newTransaction,
    editTransaction,
    newMember,
    newLedger
  }
}

