import React, { useEffect, useRef, useState } from 'react'
import cytoscape from 'cytoscape'
import { getJSON } from './api.js'
import { formatAmount, formatTime } from './format.js'

const style = [
  {
    selector: 'node',
    style: {
      'background-color': '#7b69c5',
      'border-width': 3,
      'border-color': '#ddd5f8',
      width: 32,
      height: 32,
      label: 'data(label)',
      'font-family': 'Inter Variable, sans-serif',
      'font-weight': 600,
      'font-size': 13,
      color: '#28234b',
      'text-valign': 'bottom',
      'text-margin-y': 6,
      'text-wrap': 'ellipsis',
      'text-max-width': 145,
      'text-background-color': '#f8f7fc',
      'text-background-opacity': 1,
      'text-background-padding': 2,
    },
  },
  {
    selector: 'edge',
    style: {
      width: 2,
      'curve-style': 'bezier',
      'control-point-step-size': 52,
      'line-color': '#8b83ac',
      'target-arrow-color': '#736794',
      'target-arrow-shape': 'triangle',
      'arrow-scale': 1.15,
      label: 'data(label)',
      'font-family': 'Inter Variable, sans-serif',
      'font-size': 10,
      color: '#4e466d',
      'text-background-color': '#f8f7fc',
      'text-background-opacity': 1,
      'text-background-padding': 2,
      'text-events': 'yes',
    },
  },
  {
    selector: 'edge:loop',
    style: { 'loop-direction': '-35deg', 'loop-sweep': '75deg', 'control-point-step-size': 65 },
  },
  {
    selector: 'node:selected',
    style: { 'background-color': '#5142a1', 'border-color': '#a995ee', 'border-width': 5 },
  },
  {
    selector: 'edge:selected',
    style: {
      width: 4,
      color: '#32246d',
      'line-color': '#5142a1',
      'target-arrow-color': '#5142a1',
      label: 'data(transactionID)',
    },
  },
]

const nodeID = accountID => `account:${accountID}`
const edgeID = index => `transfer:${index}`

function GraphDetails({ snapshot, selected, chooseNode, chooseEdge, openHistory }) {
  const account = selected?.type === 'account' ? selected.record : null
  const transfer = selected?.type === 'transfer' ? selected.record : null
  const nameOf = id => snapshot.accounts.find(item => item.account_id === id)?.name ?? id
  const connected = account ? snapshot.transfers
    .map((item, index) => ({ item, index }))
    .filter(({ item }) => item.from_account_id === account.account_id || item.to_account_id === account.account_id) : []

  return (
    <aside className="graph-details" aria-label="Сведения о вершине или ребре">
      <label htmlFor="graph-account">Аккаунт на графе</label>
      <select id="graph-account" value={account?.account_id ?? ''} onChange={event => chooseNode(event.target.value)}>
        <option value="">Выберите аккаунт</option>
        {snapshot.accounts.map(item => (
          <option key={item.account_id} value={item.account_id}>
            {item.name === item.account_id ? item.name : `${item.name} (${item.account_id})`}
          </option>
        ))}
      </select>
      {!selected && <p className="notice">Нажмите на вершину или стрелку, чтобы посмотреть детали.</p>}
      {account && (
        <div className="graph-selection">
          <h3>{account.name}</h3>
          <code>{account.account_id}</code>
          {connected.length > 0 && (
            <div className="connected-transfers">
              <label htmlFor="graph-transfer">Переводы аккаунта</label>
              <select id="graph-transfer" value="" onChange={event => chooseEdge(event.target.value)}>
                <option value="">Выберите перевод</option>
                {connected.map(({ item, index }) => (
                  <option key={index} value={index}>
                    {item.transaction_id} · {formatAmount(item.amount_kopecks)}
                  </option>
                ))}
              </select>
            </div>
          )}
          <button type="button" className="secondary" onClick={() => openHistory(account)}>Открыть историю</button>
        </div>
      )}
      {transfer && (
        <div className="graph-selection">
          <h3>{formatAmount(transfer.amount_kopecks)}</h3>
          <code>{transfer.transaction_id}</code>
          <dl className="graph-transfer-info">
            <div><dt>Отправитель</dt><dd>{nameOf(transfer.from_account_id)}</dd></div>
            <div><dt>Получатель</dt><dd>{nameOf(transfer.to_account_id)}</dd></div>
            <div><dt>Время (UTC)</dt><dd>{formatTime(transfer.time)}</dd></div>
          </dl>
        </div>
      )}
    </aside>
  )
}

export default function Graph({ openHistory }) {
  const [revision, setRevision] = useState(0)
  const [status, setStatus] = useState('loading')
  const [snapshot, setSnapshot] = useState(null)
  const [selected, setSelected] = useState(null)
  const container = useRef(null)
  const instance = useRef(null)

  useEffect(() => {
    const controller = new AbortController()
    getJSON('/api/v1/graph', controller.signal)
      .then(graph => {
        if (controller.signal.aborted) return
        graph.accounts.sort((left, right) => left.name.localeCompare(right.name, 'ru'))
        graph.transfers.sort((left, right) => left.transaction_id.localeCompare(right.transaction_id))
        setSnapshot(graph)
        setStatus('ready')
      })
      .catch(error => {
        if (!controller.signal.aborted) setStatus(error.status === 422 ? 'too-large' : 'error')
      })
    return () => controller.abort()
  }, [revision])

  useEffect(() => {
    if (!snapshot?.accounts.length || !container.current) return
    const nodes = snapshot.accounts.map(account => ({
      data: { id: nodeID(account.account_id), label: account.name, record: account },
    }))
    const edges = snapshot.transfers.map((transfer, index) => ({
      data: {
        id: edgeID(index),
        source: nodeID(transfer.from_account_id),
        target: nodeID(transfer.to_account_id),
        label: snapshot.transfers.length <= 50 ? transfer.transaction_id : '',
        transactionID: transfer.transaction_id,
        record: transfer,
      },
    }))
    const cy = cytoscape({
      container: container.current,
      elements: [...nodes, ...edges],
      style,
      minZoom: 0.02,
      maxZoom: 2.5,
      selectionType: 'single',
      layout: nodes.length <= 20 ? {
        name: 'circle',
        animate: false,
        padding: 68,
      } : {
        name: 'cose',
        animate: false,
        padding: 50,
        nodeRepulsion: 11000,
        idealEdgeLength: 155,
        nodeOverlap: 28,
        componentSpacing: 100,
        numIter: 350,
      },
    })
    instance.current = cy
    cy.on('select', 'node, edge', event => {
      const element = event.target
      setSelected({ type: element.isNode() ? 'account' : 'transfer', record: element.data('record') })
    })
    cy.on('tap', event => {
      if (event.target === cy) {
        cy.elements(':selected').unselect()
        setSelected(null)
      }
    })
    return () => {
      instance.current = null
      cy.destroy()
    }
  }, [snapshot])

  function refresh() {
    setStatus('loading')
    setSnapshot(null)
    setSelected(null)
    setRevision(value => value + 1)
  }

  function choose(elementID) {
    const cy = instance.current
    if (!cy) return
    cy.elements(':selected').unselect()
    if (!elementID) {
      setSelected(null)
      return
    }
    const element = cy.getElementById(elementID)
    element.select()
    cy.center(element)
  }

  function zoom(factor) {
    const cy = instance.current
    if (!cy) return
    cy.zoom({
      level: cy.zoom() * factor,
      renderedPosition: { x: cy.width() / 2, y: cy.height() / 2 },
    })
  }

  return (
    <section className="panel graph-panel" aria-labelledby="graph-title">
      <header className="graph-heading">
        <div>
          <h2 id="graph-title">Весь граф</h2>
          {snapshot && <p>Аккаунтов: {snapshot.accounts.length} · Переводов: {snapshot.transfers.length}</p>}
        </div>
        <button className="secondary" type="button" onClick={refresh} disabled={status === 'loading'}>Обновить</button>
      </header>
      {status === 'loading' && <p className="graph-status" role="status">Загрузка графа…</p>}
      {status === 'error' && <p className="graph-status" role="alert">Не удалось загрузить граф. Попробуйте обновить.</p>}
      {status === 'too-large' && <p className="graph-status" role="alert">Граф превышает лимит отображения. Историю отдельного аккаунта можно открыть во вкладке «История».</p>}
      {snapshot?.accounts.length === 0 && <p className="graph-status">В графе пока нет аккаунтов.</p>}
      {snapshot?.accounts.length > 0 && (
        <div className="graph-workspace">
          <div className="graph-viewport">
            <div className="graph-canvas" ref={container} role="img" aria-label="Аккаунты и направленные переводы между ними" />
            <div className="graph-controls" role="group" aria-label="Масштаб графа">
              <button type="button" onClick={() => zoom(1 / 1.25)} aria-label="Уменьшить">−</button>
              <button type="button" onClick={() => zoom(1.25)} aria-label="Увеличить">+</button>
              <button type="button" onClick={() => instance.current?.fit(undefined, 50)}>Вместить</button>
            </div>
          </div>
          <GraphDetails
            snapshot={snapshot}
            selected={selected}
            chooseNode={id => choose(id ? nodeID(id) : '')}
            chooseEdge={index => choose(index !== '' ? edgeID(index) : '')}
            openHistory={openHistory}
          />
        </div>
      )}
    </section>
  )
}
