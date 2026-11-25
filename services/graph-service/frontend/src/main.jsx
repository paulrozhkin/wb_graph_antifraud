import React, { lazy, Suspense, useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import '@fontsource-variable/inter'
import { getJSON } from './api.js'
import { formatAmount, formatTime } from './format.js'
import './styles.css'

const Graph = lazy(() => import('./Graph.jsx'))

function GraphMark() {
  return <span className="graph-mark" aria-hidden="true"><i /><i /><i /></span>
}

function Avatar({ name }) {
  return <span className="avatar" aria-hidden="true">{Array.from(name)[0]}</span>
}

function direction(transfer, id) {
  if (transfer.from_account_id === id && transfer.to_account_id === id) return 'self'
  return transfer.from_account_id === id ? 'outgoing' : 'incoming'
}

const directions = {
  incoming: { label: 'Входящий', symbol: '↙' },
  outgoing: { label: 'Исходящий', symbol: '↗' },
  self: { label: 'Себе', symbol: '↻' },
}

function History({ account, state, retry }) {
  if (!account) {
    return (
      <section className="panel history-placeholder">
        <GraphMark />
        <h2>Выберите аккаунт</h2>
      </section>
    )
  }

  const transfers = state.items
  const counters = { incoming: 0, outgoing: 0, self: 0 }
  for (const transfer of transfers) counters[direction(transfer, account.account_id)]++

  return (
    <section className="panel history" aria-labelledby="account-title">
      <header className="account-header">
        <Avatar name={account.name} />
        <div>
          <h2 id="account-title">{account.name}</h2>
          <code>{account.account_id}</code>
        </div>
      </header>
      <div className="history-body">
        <h3>Последние переводы</h3>
        {state.status === 'loading' && <p className="notice" role="status">Загрузка переводов…</p>}
        {state.status === 'error' && (
          <div className="notice" role="alert">
            <p>Не удалось загрузить переводы.</p>
            <button className="secondary" type="button" onClick={retry}>Повторить</button>
          </div>
        )}
        {state.status === 'ready' && transfers.length === 0 && <p className="notice">Переводов пока нет.</p>}
        {state.status === 'ready' && transfers.length > 0 && (
          <>
            <dl className="transfer-counts">
              <div><dt>Входящие</dt><dd>{counters.incoming}</dd></div>
              <div><dt>Исходящие</dt><dd>{counters.outgoing}</dd></div>
              <div><dt>Себе</dt><dd>{counters.self}</dd></div>
            </dl>
            <div className="table-scroll" tabIndex={0} role="region" aria-label="Таблица переводов">
              <table>
                <thead>
                  <tr><th>Операция</th><th>Отправитель</th><th>Получатель</th><th>Время (UTC)</th><th className="amount">Сумма</th></tr>
                </thead>
                <tbody>
                  {transfers.map(transfer => {
                    const type = direction(transfer, account.account_id)
                    const { label, symbol } = directions[type]
                    return (
                      <tr key={transfer.transaction_id}>
                        <td>
                          <span className={`direction ${type}`}><span aria-hidden="true">{symbol}</span> {label}</span>
                          <code>{transfer.transaction_id}</code>
                        </td>
                        <td>{transfer.from_account_id}</td>
                        <td>{transfer.to_account_id}</td>
                        <td><time dateTime={transfer.time}>{formatTime(transfer.time)}</time></td>
                        <td className="amount">{formatAmount(transfer.amount_kopecks)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </section>
  )
}

function App() {
  const [view, setView] = useState('graph')
  const [query, setQuery] = useState('')
  const [request, setRequest] = useState(null)
  const [search, setSearch] = useState({ status: 'idle', items: [] })
  const [selection, setSelection] = useState(null)
  const [history, setHistory] = useState({ status: 'idle', items: [] })

  function selectAccount(account) {
    setSelection({ account })
    setHistory({ status: 'loading', items: [] })
  }

  useEffect(() => {
    if (!request) return
    const controller = new AbortController()
    getJSON(`/api/v1/accounts?q=${encodeURIComponent(request.prefix)}`, controller.signal)
      .then(items => {
        if (controller.signal.aborted) return
        setSearch({ status: 'ready', items })
        if (items.length > 0) selectAccount(items[0])
      })
      .catch(() => {
        if (!controller.signal.aborted) setSearch({ status: 'error', items: [] })
      })
    return () => controller.abort()
  }, [request])

  useEffect(() => {
    if (!selection) return
    const controller = new AbortController()
    const id = encodeURIComponent(selection.account.account_id)
    getJSON(`/api/v1/accounts/${id}/transfers`, controller.signal)
      .then(items => {
        if (!controller.signal.aborted) setHistory({ status: 'ready', items })
      })
      .catch(() => {
        if (!controller.signal.aborted) setHistory({ status: 'error', items: [] })
      })
    return () => controller.abort()
  }, [selection])

  function submitSearch(event) {
    event.preventDefault()
    const prefix = query.trim()
    const length = Array.from(prefix).length
    if (length < 2 || length > 100 || /\s/u.test(prefix)) {
      setRequest(null)
      setSearch({ status: 'invalid', items: [] })
    } else {
      setRequest({ prefix })
      setSearch({ status: 'loading', items: [] })
    }
    setSelection(null)
  }

  const account = selection?.account
  return (
    <>
      <header className="topbar">
        <div className="shell brand"><GraphMark /><span>Граф переводов</span></div>
      </header>
      <main className="shell workspace">
        <div className="workspace-title">
          <h1>Связи аккаунтов</h1>
          <div className="view-controls" role="group" aria-label="Раздел">
            <button type="button" aria-pressed={view === 'graph'} onClick={() => setView('graph')}>Граф</button>
            <button type="button" aria-pressed={view === 'history'} onClick={() => setView('history')}>История</button>
          </div>
        </div>
        {view === 'graph' && (
          <Suspense fallback={<p className="graph-status" role="status">Загрузка графа…</p>}>
            <Graph openHistory={item => {
              selectAccount(item)
              setView('history')
            }} />
          </Suspense>
        )}
        {view === 'history' && (
          <div className="workspace-grid">
            <section className="panel search" aria-labelledby="search-title">
              <h2 id="search-title">Поиск аккаунта</h2>
              <form onSubmit={submitSearch}>
                <label htmlFor="account-query">Начало имени</label>
                <div className="search-field">
                  <input
                    id="account-query"
                    type="search"
                    placeholder="Например, Ве"
                    value={query}
                    onChange={event => setQuery(event.target.value)}
                    autoComplete="off"
                    aria-invalid={search.status === 'invalid'}
                  />
                  <button type="submit">Найти</button>
                </div>
              </form>
              {search.status === 'invalid' && <p className="notice" role="alert">Введите от 2 до 100 символов без пробелов.</p>}
              {search.status === 'loading' && <p className="notice" role="status">Поиск…</p>}
              {search.status === 'error' && <p className="notice" role="alert">Поиск недоступен. Повторите запрос.</p>}
              {search.status === 'ready' && search.items.length === 0 && <p className="notice" role="status">Аккаунты не найдены.</p>}
              {search.items.length > 0 && (
                <ul className="accounts" aria-label="Найденные аккаунты">
                  {search.items.map(item => (
                    <li key={item.account_id}>
                      <button
                        type="button"
                        className="account-option"
                        aria-pressed={item.account_id === account?.account_id}
                        onClick={() => selectAccount(item)}
                      >
                        <Avatar name={item.name} />
                        <span><strong>{item.name}</strong><code>{item.account_id}</code></span>
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </section>
            <History account={account} state={history} retry={() => selectAccount(account)} />
          </div>
        )}
      </main>
    </>
  )
}

createRoot(document.getElementById('root')).render(<App />)
