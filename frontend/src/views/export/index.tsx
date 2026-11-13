import { useCallback, useEffect, useMemo, useState } from 'react'

import { fetchJson, request } from '@/api/client'

const ENDPOINT = '/api/export'

type Stage = '送达证明' | '完整性验证' | '接收确认'

type MissingProof = { stage: Stage; rank: number; reason: string }

type ReceiptFlag = { code: string; label: string; detail: string }

type Receipt = {
  id: number
  stage: Stage
  verdict: string
  attempt_no: number | null
  recorded_at: string
  source: string
  channel_identity: string
  note: string
  evidence: Record<string, unknown>
  origin: string
  role: 'evidence' | 'supporting' | 'effective' | 'ignored' | 'duplicate'
  valid: boolean
  flags: ReceiptFlag[]
}

type EffectiveResult = {
  status?: string
  stage?: Stage | null
  verdict?: string
  receipt_id?: number | null
  recorded_at?: string | null
  source?: string | null
  landing_ref?: string | null
  reason: string
}

type Reconciliation = {
  status: string
  status_reason: string
  stage_reached: { stage: Stage | null; rank: number }
  missing_proofs: MissingProof[]
  effective_result: EffectiveResult | null
  copy_count: number
  transmission_count: number
  confirmation_count: number
  ignored_receipts: Receipt[]
  duplicate_receipts: Receipt[]
  rejection_stage: Stage | null
  rejection_receipt_id: number | null
  sender_ack: { values: (number | null)[]; note: string }
  receipts: Receipt[]
}

type EnvelopeListItem = {
  id: number
  envelope_no: string
  delivery_identity: string
  dataset: string
  batch_no: string
  channel_no: string
  channel_name: string
  channel_identity: string
  target_system: string
  encoding: string
  digest: string
  short_digest: string
  sealed_at: string
  status: string
  status_reason: string
  stage_reached: { stage: Stage | null; rank: number }
  missing_stages: Stage[]
  rejection_stage: Stage | null
  transmission_count: number
  copy_count: number
  receiver_landed: boolean
  confirmations: number
}

type Attempt = {
  attempt_no: number
  transport_id: string
  sent_at: string
  sender_status: number
  note?: string
}

type EnvelopeDetail = EnvelopeListItem & {
  batch_id: number
  channel_protocol: string
  byte_size: number
  row_count: number
  sealed_by: string
  simulation: string
  receiver_landed: boolean
  landing_ref: string | null
  attempts: Attempt[]
  actions: { at: string; by: string; action: string; detail: string }[]
  seal_note: string
  snapshot: {
    rows: Record<string, unknown>[]
    row_count: number
    byte_size: number
    digest: string
    digest_recomputed: string
    digest_matches: boolean
    batch_current_rows: number
    batch_drift_rows: number
    batch_prefix_unchanged: boolean
    note: string
  }
  receipts: Receipt[]
  receipt_timeline: Receipt[]
  reconciliation: Reconciliation
}

type Batch = {
  id: number
  batch_no: string
  dataset: string
  storage_path: string
  file_format: string
  encoding: string
  confirmed_at: string
  row_count: number
  digest: string
}

type Channel = {
  id: number
  '通道编号': string
  '通道名称': string
  '通道身份': string
  '目标系统': string
  '分发格式': string
  '传输协议': string
  '状态': string
}

type Options = {
  encodings: string[]
  simulations: string[]
  statuses: string[]
  stages: Stage[]
  stage_verdicts: Record<Stage, string[]>
}

type Stats = {
  total: number
  status_counts: Record<string, number>
  pending_missing: number
  transmissions: number
  copies: number
}

const STAGE_TONE: Record<Stage, string> = {
  送达证明: 'stage-delivery',
  完整性验证: 'stage-integrity',
  接收确认: 'stage-confirm',
}

const STATUS_TONE: Record<string, string> = {
  待投递: 'status-idle',
  待补证: 'status-warn',
  已落盘待确认: 'status-warn',
  已确认接收: 'status-ok',
  已拒收: 'status-bad',
  已遗失: 'status-bad',
}

const MISSING_OPTIONS = [
  { value: '', label: '不限缺失证明' },
  { value: '送达证明', label: '缺送达证明' },
  { value: '完整性验证', label: '缺完整性验证' },
  { value: '接收确认', label: '缺接收确认' },
]

const SORT_OPTIONS = [
  { value: 'stage_desc', label: '证明阶段：高→低' },
  { value: 'stage_asc', label: '证明阶段：低→高' },
  { value: 'time_desc', label: '封存时间：新→旧' },
  { value: 'time_asc', label: '封存时间：旧→新' },
]

function formatTime(value: string | null | undefined): string {
  if (!value) return '—'
  // 后端时间均为带时区的 ISO 字符串，这里只截取到分钟，保留日期与时区标记。
  return value.replace('T', ' ').slice(0, 16)
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`
  return `${(value / 1024).toFixed(1)} KB`
}

function roleLabel(role: Receipt['role']): string {
  switch (role) {
    case 'effective': return '有效结论'
    case 'ignored': return '未参与对账'
    case 'duplicate': return '重复回执'
    case 'supporting': return '佐证'
    default: return '回执'
  }
}

export default function ExportView() {
  const [stats, setStats] = useState<Stats | null>(null)
  const [options, setOptions] = useState<Options | null>(null)
  const [channels, setChannels] = useState<Channel[]>([])
  const [batches, setBatches] = useState<Batch[]>([])
  const [envelopes, setEnvelopes] = useState<EnvelopeListItem[]>([])
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [detail, setDetail] = useState<EnvelopeDetail | null>(null)
  const [message, setMessage] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [busy, setBusy] = useState(false)

  const [channelFilter, setChannelFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [missingFilter, setMissingFilter] = useState('')
  const [sort, setSort] = useState('stage_desc')
  const [timelineSort, setTimelineSort] = useState<'time' | 'stage'>('time')

  const [showSeal, setShowSeal] = useState(false)
  const [sealBatch, setSealBatch] = useState('')
  const [sealChannel, setSealChannel] = useState('')
  const [sealEncoding, setSealEncoding] = useState('UTF-8')
  const [sealSimulation, setSealSimulation] = useState('正常签收')

  const [receiptStage, setReceiptStage] = useState<Stage>('送达证明')
  const [receiptVerdict, setReceiptVerdict] = useState('已送达')
  const [receiptNote, setReceiptNote] = useState('')
  const [receiptChannel, setReceiptChannel] = useState('')

  const loadReference = useCallback(async () => {
    try {
      const [statsPayload, optionsPayload, channelPayload, batchPayload] = await Promise.all([
        fetchJson<Stats>(`${ENDPOINT}/stats`),
        fetchJson<Options>(`${ENDPOINT}/options`),
        fetchJson<{ items: Channel[] }>(`${ENDPOINT}/channels`),
        fetchJson<{ items: Batch[] }>(`${ENDPOINT}/batches`),
      ])
      setStats(statsPayload)
      setOptions(optionsPayload)
      setChannels(channelPayload.items ?? [])
      setBatches(batchPayload.items ?? [])
      setSealBatch((prev) => prev || String((batchPayload.items ?? [])[0]?.id ?? ''))
      setSealChannel((prev) => prev || String((channelPayload.items ?? [])[0]?.id ?? ''))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '对账基础信息读取失败')
    }
  }, [])

  const loadEnvelopes = useCallback(async () => {
    const query = new URLSearchParams()
    if (channelFilter) query.set('channel', channelFilter)
    if (statusFilter) query.set('status', statusFilter)
    if (missingFilter) query.set('missing', missingFilter)
    query.set('sort', sort)
    try {
      const payload = await fetchJson<{ items: EnvelopeListItem[]; total: number }>(
        `${ENDPOINT}/envelopes?${query.toString()}`,
      )
      setEnvelopes(payload.items ?? [])
      setSelectedId((prev) => {
        if (prev && (payload.items ?? []).some((item) => item.id === prev)) return prev
        return payload.items?.[0]?.id ?? null
      })
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '交付信封列表读取失败')
    }
  }, [channelFilter, statusFilter, missingFilter, sort])

  const loadDetail = useCallback(async () => {
    if (selectedId == null) {
      setDetail(null)
      return
    }
    try {
      const payload = await fetchJson<EnvelopeDetail>(`${ENDPOINT}/envelopes/${selectedId}`)
      setDetail(payload)
    } catch (error) {
      setDetail(null)
      setErrorMessage(error instanceof Error ? error.message : '信封明细读取失败')
    }
  }, [selectedId])

  useEffect(() => {
    void loadReference()
  }, [loadReference])

  useEffect(() => {
    void loadEnvelopes()
  }, [loadEnvelopes])

  useEffect(() => {
    void loadDetail()
  }, [loadDetail])

  const refreshAll = useCallback(async () => {
    await Promise.all([loadReference(), loadEnvelopes(), loadDetail()])
  }, [loadReference, loadEnvelopes, loadDetail])

  async function runAction(path: string, body: Record<string, unknown>, actionLabel: string) {
    setBusy(true)
    setMessage('')
    setErrorMessage('')
    try {
      const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
      const payload = (await response.json()) as { ok: boolean; message: string; entry?: unknown }
      if (!response.ok) {
        throw new Error(payload.message || `${actionLabel}未生效`)
      }
      if (!payload.ok) {
        setErrorMessage(payload.message)
      } else {
        setMessage(payload.message)
      }
      await refreshAll()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : `${actionLabel}失败`)
    } finally {
      setBusy(false)
    }
  }

  async function sealEnvelope() {
    if (!sealBatch || !sealChannel) {
      setErrorMessage('请选择数据落盘确认批次与目标通道')
      return
    }
    await runAction(`${ENDPOINT}/envelopes`, {
      batch_id: Number(sealBatch),
      channel_id: Number(sealChannel),
      encoding: sealEncoding,
      simulation: sealSimulation,
      sealed_by: '值班管理员',
    }, '封存信封')
    setShowSeal(false)
  }

  const missingOptions = useMemo(() => MISSING_OPTIONS, [])
  const stageVerdicts = options?.stage_verdicts?.[receiptStage] ?? ['已送达', '遗失', '拒收']

  useEffect(() => {
    if (!stageVerdicts.includes(receiptVerdict)) {
      setReceiptVerdict(stageVerdicts[0] ?? '')
    }
  }, [stageVerdicts, receiptVerdict])

  const timeline = useMemo(() => {
    if (!detail) return []
    const rows = [...detail.receipts]
    if (timelineSort === 'stage') {
      const rank: Record<Stage, number> = { 送达证明: 1, 完整性验证: 2, 接收确认: 3 }
      rows.sort((a, b) =>
        rank[a.stage] - rank[b.stage] || a.recorded_at.localeCompare(b.recorded_at) || a.id - b.id,
      )
    } else {
      rows.sort((a, b) => a.recorded_at.localeCompare(b.recorded_at) || a.id - b.id)
    }
    return rows
  }, [detail, timelineSort])

  const reconciliation = detail?.reconciliation
  const missingAcceptance = reconciliation?.missing_proofs.some((item) => item.stage === '接收确认') ?? false
  const alreadyRejected = reconciliation?.status === '已拒收'
  const alreadyAccepted = reconciliation?.status === '已确认接收'
  const canSend = Boolean(detail) && !alreadyRejected && !alreadyAccepted
  const sendLabel = detail && detail.attempts.length > 0 ? '原样重送' : '投递接收端'
  const canSupplement = Boolean(detail) && detail!.receiver_landed && missingAcceptance && !alreadyRejected && !alreadyAccepted

  return (
    <section className="page" data-module="export">
      <header className="page-head">
        <div>
          <h2>交付信封对账</h2>
          <p className="page-desc">
            从数据落盘的确认批次取内容封存信封，再向本地模拟接收端投递；交付是否完成只看接收端回执，
            发送端 200 不能作为交付结论。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setShowSeal((prev) => !prev)}>
            {showSeal ? '收起封存表单' : '封存交付信封'}
          </button>
          <button className="btn ghost" type="button" onClick={() => void refreshAll()}>刷新对账</button>
        </div>
      </header>

      {showSeal && (
        <form
          className="seal-form"
          onSubmit={(event) => { event.preventDefault(); void sealEnvelope() }}
        >
          <label className="filter-item">
            <span>落盘确认批次</span>
            <select value={sealBatch} onChange={(event) => setSealBatch(event.target.value)}>
              {batches.map((batch) => (
                <option key={batch.id} value={batch.id}>
                  {batch.batch_no} · {batch.dataset} · {batch.row_count} 行 · {batch.file_format}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-item">
            <span>目标通道</span>
            <select value={sealChannel} onChange={(event) => setSealChannel(event.target.value)}>
              {channels.map((channel) => (
                <option key={channel.id} value={channel.id}>
                  {channel['通道编号']} · {channel['通道名称']} · {channel['通道身份']}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-item">
            <span>编码格式</span>
            <select value={sealEncoding} onChange={(event) => setSealEncoding(event.target.value)}>
              {(options?.encodings ?? ['UTF-8']).map((encoding) => (
                <option key={encoding} value={encoding}>{encoding}</option>
              ))}
            </select>
          </label>
          <label className="filter-item">
            <span>接收端演练场景</span>
            <select value={sealSimulation} onChange={(event) => setSealSimulation(event.target.value)}>
              {(options?.simulations ?? ['正常签收']).map((simulation) => (
                <option key={simulation} value={simulation}>{simulation}</option>
              ))}
            </select>
          </label>
          <button className="btn primary" type="submit" disabled={busy}>封存并生成摘要</button>
        </form>
      )}

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">信封总数</span>
          <strong className="stat-value">{stats?.total ?? 0}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">已确认接收</span>
          <strong className="stat-value">{stats?.status_counts?.['已确认接收'] ?? 0}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">尚缺证明</span>
          <strong className="stat-value">{stats?.pending_missing ?? 0}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">传输次数 / 有效数据份数</span>
          <strong className="stat-value">
            {stats?.transmissions ?? 0} / {stats?.copies ?? 0}
          </strong>
        </article>
      </div>

      {(message || errorMessage) && (
        <div className={`notice ${errorMessage ? 'notice-error' : 'notice-ok'}`}>
          {errorMessage || message}
        </div>
      )}

      <div className="reconcile-layout">
        <aside className="envelope-rail">
          <div className="rail-filters">
            <label className="filter-item">
              <span>通道身份</span>
              <select value={channelFilter} onChange={(event) => setChannelFilter(event.target.value)}>
                <option value="">全部通道</option>
                {channels.map((channel) => (
                  <option key={channel.id} value={channel['通道身份']}>
                    {channel['通道身份']} · {channel['通道名称']}
                  </option>
                ))}
              </select>
            </label>
            <label className="filter-item">
              <span>交付状态</span>
              <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                <option value="">全部状态</option>
                {(options?.statuses ?? []).map((status) => (
                  <option key={status} value={status}>{status}</option>
                ))}
              </select>
            </label>
            <label className="filter-item">
              <span>缺失证明</span>
              <select value={missingFilter} onChange={(event) => setMissingFilter(event.target.value)}>
                {missingOptions.map((item) => (
                  <option key={item.value} value={item.value}>{item.label}</option>
                ))}
              </select>
            </label>
            <label className="filter-item">
              <span>排序</span>
              <select value={sort} onChange={(event) => setSort(event.target.value)}>
                {SORT_OPTIONS.map((item) => (
                  <option key={item.value} value={item.value}>{item.label}</option>
                ))}
              </select>
            </label>
          </div>

          <ul className="envelope-list">
            {envelopes.map((item) => (
              <li key={item.id}>
                <button
                  type="button"
                  className={`envelope-card ${item.id === selectedId ? 'active' : ''}`}
                  onClick={() => setSelectedId(item.id)}
                >
                  <div className="envelope-card-head">
                    <span className="envelope-no">{item.envelope_no}</span>
                    <span className={`status-chip ${STATUS_TONE[item.status] ?? 'status-idle'}`}>
                      {item.status}
                    </span>
                  </div>
                  <div className="envelope-card-body">
                    <span>{item.dataset}</span>
                    <span className="muted">{item.channel_identity} · {item.target_system}</span>
                  </div>
                  <div className="envelope-card-foot">
                    <span>已至：{item.stage_reached.stage ?? '未投递'}</span>
                    <span>传输 {item.transmission_count} 次 / 数据 {item.copy_count} 份</span>
                  </div>
                  {item.missing_stages.length > 0 && (
                    <div className="missing-inline">缺：{item.missing_stages.join('、')}</div>
                  )}
                </button>
              </li>
            ))}
            {envelopes.length === 0 && <li className="empty-state">当前筛选条件下没有交付信封</li>}
          </ul>
        </aside>

        <main className="reconcile-panel">
          {!detail && <p className="empty-state">左侧选择一份信封查看送达证明与接收确认对账。</p>}
          {detail && reconciliation && (
            <>
              <div className="panel-head">
                <div>
                  <h3>{detail.envelope_no}</h3>
                  <p className="muted">
                    交付身份 {detail.delivery_identity} · 封存于 {formatTime(detail.sealed_at)} · 封存人 {detail.sealed_by}
                  </p>
                </div>
                <span className={`status-chip large ${STATUS_TONE[reconciliation.status] ?? 'status-idle'}`}>
                  {reconciliation.status}
                </span>
              </div>

              <div className={`verdict-banner ${alreadyAccepted ? 'ok' : alreadyRejected ? 'bad' : 'warn'}`}>
                <strong>{reconciliation.status_reason}</strong>
                <span>{reconciliation.sender_ack.note}</span>
                <span>发送端响应：{reconciliation.sender_ack.values.map((value) => value ?? '—').join('、')}</span>
              </div>

              <div className="meta-grid">
                <div><span>目标系统</span><strong>{detail.target_system}</strong></div>
                <div><span>通道身份</span><strong>{detail.channel_identity}</strong></div>
                <div><span>通道 / 协议</span><strong>{detail.channel_no} · {detail.channel_protocol}</strong></div>
                <div><span>编码格式</span><strong>{detail.encoding}</strong></div>
                <div><span>封存摘要</span><strong className="mono" title={detail.digest}>{detail.short_digest}</strong></div>
                <div><span>行数 / 字节</span><strong>{detail.row_count} 行 · {formatBytes(detail.byte_size)}</strong></div>
                <div><span>来源落盘批次</span><strong>{detail.batch_no}</strong></div>
                <div><span>接收端演练场景</span><strong>{detail.simulation}</strong></div>
                <div><span>接收端落盘编号</span><strong className="mono">{detail.landing_ref ?? '尚未落盘'}</strong></div>
                <div><span>传输 / 数据份数</span><strong>{reconciliation.transmission_count} 次 / {reconciliation.copy_count} 份</strong></div>
              </div>

              <section className="snapshot-box">
                <div className="section-title">封存快照</div>
                <p className="muted">{detail.seal_note}</p>
                <div className="snapshot-facts">
                  <span>快照 {detail.snapshot.row_count} 行</span>
                  <span>当前批次 {detail.snapshot.batch_current_rows} 行</span>
                  <span>封存后新增 {detail.snapshot.batch_drift_rows} 行</span>
                  <span className={detail.snapshot.digest_matches ? 'ok-text' : 'error-text'}>
                    {detail.snapshot.digest_matches ? '重算摘要一致' : '重算摘要不一致'}
                  </span>
                  <span>{detail.snapshot.note}</span>
                </div>
                <table className="snapshot-table">
                  <thead>
                    <tr>
                      {Object.keys(detail.snapshot.rows[0] ?? {}).slice(0, 6).map((key) => <th key={key}>{key}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {detail.snapshot.rows.slice(0, 4).map((row, index) => (
                      <tr key={index}>
                        {Object.keys(detail.snapshot.rows[0] ?? {}).slice(0, 6).map((key) => (
                          <td key={key}>{String(row[key] ?? '—')}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>

              <section className="proof-strip">
                {(['送达证明', '完整性验证', '接收确认'] as Stage[]).map((stage) => {
                  const missing = reconciliation.missing_proofs.find((item) => item.stage === stage)
                  const rejectedHere = reconciliation.rejection_stage === stage
                  const confirmations = reconciliation.receipts.filter(
                    (receipt) => receipt.stage === stage && receipt.valid && receipt.role !== 'ignored',
                  )
                  const state = rejectedHere ? '已拒收' : missing ? '缺失' : confirmations.length > 0 ? '已取得' : '未发生'
                  return (
                    <div key={stage} className={`proof-cell ${missing ? 'missing' : rejectedHere ? 'rejected' : 'done'}`}>
                      <span className={`stage-chip ${STAGE_TONE[stage]}`}>{stage}</span>
                      <strong>{state}</strong>
                      <span className="muted">
                        {missing ? missing.reason : rejectedHere ? '该阶段作出最终拒收' : '本通道有效回执已到齐'}
                      </span>
                    </div>
                  )
                })}
              </section>

              {(reconciliation.missing_proofs.length > 0 || reconciliation.ignored_receipts.length > 0) && (
                <section className="attention">
                  {reconciliation.missing_proofs.length > 0 && (
                    <>
                      <div className="section-title">尚缺哪种证明</div>
                      <ul>
                        {reconciliation.missing_proofs.map((item) => (
                          <li key={item.stage}>
                            <span className={`stage-chip ${STAGE_TONE[item.stage]}`}>{item.stage}</span>
                            <span>{item.reason}</span>
                            <button
                              type="button"
                              className="link"
                              onClick={() => { setMissingFilter(item.stage); setStatusFilter('') }}
                            >
                              定位同类信封
                            </button>
                          </li>
                        ))}
                      </ul>
                    </>
                  )}
                  {reconciliation.ignored_receipts.length > 0 && (
                    <>
                      <div className="section-title">未参与对账的回执</div>
                      <ul>
                        {reconciliation.ignored_receipts.map((receipt) => (
                          <li key={receipt.id}>
                            <span className="mono">#{receipt.id}</span>
                            <span>{receipt.stage} · {receipt.verdict}</span>
                            <span className="muted">
                              {receipt.flags.map((flag) => `${flag.label}：${flag.detail}`).join('；')}
                            </span>
                          </li>
                        ))}
                      </ul>
                    </>
                  )}
                </section>
              )}

              <section className="receipt-layer">
                <div className="section-head">
                  <div>
                    <div className="section-title">接收端回执</div>
                    <p className="muted">送达证明、完整性验证、接收确认三类回执按时间展开；可切换为按证明阶段排序。</p>
                  </div>
                  <div className="segmented">
                    <button
                      type="button"
                      className={timelineSort === 'time' ? 'active' : ''}
                      onClick={() => setTimelineSort('time')}
                    >
                      按时间
                    </button>
                    <button
                      type="button"
                      className={timelineSort === 'stage' ? 'active' : ''}
                      onClick={() => setTimelineSort('stage')}
                    >
                      按证明阶段
                    </button>
                  </div>
                </div>

                <ol className="timeline">
                  {timeline.map((receipt) => (
                    <li key={receipt.id} className={`timeline-item ${receipt.role}`}>
                      <div className="timeline-mark">
                        <span className={`stage-chip ${STAGE_TONE[receipt.stage]}`}>{receipt.stage}</span>
                        <span className="mono">{formatTime(receipt.recorded_at)}</span>
                      </div>
                      <div className="timeline-card">
                        <div className="timeline-head">
                          <strong>{receipt.verdict}</strong>
                          <span className={`role-chip role-${receipt.role}`}>{roleLabel(receipt.role)}</span>
                          <span className="muted">回执 #{receipt.id} · 传输 {receipt.attempt_no ?? '—'} · {receipt.source}</span>
                        </div>
                        {receipt.note && <p>{receipt.note}</p>}
                        {receipt.flags.length > 0 && (
                          <div className="flag-row">
                            {receipt.flags.map((flag) => (
                              <span key={flag.code} className="flag-chip" title={flag.detail}>{flag.label}</span>
                            ))}
                          </div>
                        )}
                        {Object.keys(receipt.evidence).length > 0 && (
                          <dl className="evidence">
                            {Object.entries(receipt.evidence).map(([key, value]) => (
                              <div key={key}>
                                <dt>{key}</dt>
                                <dd className="mono">{typeof value === 'string' ? value : JSON.stringify(value)}</dd>
                              </div>
                            ))}
                          </dl>
                        )}
                      </div>
                    </li>
                  ))}
                  {timeline.length === 0 && <li className="empty-state">接收端尚未签发任何回执</li>}
                </ol>

                {(reconciliation.duplicate_receipts.length > 0 || (reconciliation.confirmation_count > 1)) && (
                  <p className="muted duplicate-note">
                    同一交付身份收到 {reconciliation.confirmation_count} 条有效接收确认，
                    只采纳最早的 #{reconciliation.effective_result?.receipt_id ?? '—'} 为唯一有效结果；
                    重复确认不增加数据份数（当前 {reconciliation.copy_count} 份）。
                  </p>
                )}
              </section>

              <section className="attempt-layer">
                <div className="section-title">传输记录</div>
                <table className="data-table">
                  <thead>
                    <tr><th>传输批次</th><th>传输编号</th><th>发出时间</th><th>发送端响应</th><th>说明</th></tr>
                  </thead>
                  <tbody>
                    {detail.attempts.map((attempt) => (
                      <tr key={attempt.attempt_no}>
                        <td>{attempt.attempt_no}</td>
                        <td className="mono">{attempt.transport_id}</td>
                        <td>{formatTime(attempt.sent_at)}</td>
                        <td>{attempt.sender_status}</td>
                        <td>{attempt.note ?? '—'}</td>
                      </tr>
                    ))}
                    {detail.attempts.length === 0 && (
                      <tr><td className="empty-state" colSpan={5}>信封已封存，尚未投递</td></tr>
                    )}
                  </tbody>
                </table>
              </section>

              <section className="action-layer">
                <div className="section-head">
                  <div className="section-title">操作员处置</div>
                  <div className="action-buttons">
                    <button
                      className="btn primary"
                      type="button"
                      disabled={busy || !canSend}
                      onClick={() => void runAction(
                        `${ENDPOINT}/envelopes/${detail.id}/send`,
                        { operator: '值班管理员' },
                        sendLabel,
                      )}
                    >
                      {sendLabel}
                    </button>
                    <button
                      className="btn"
                      type="button"
                      title={detail.receiver_landed ? '接收端已有落盘记录，可要求补发接收确认' : '接收端尚无落盘记录，补证不成立；请原样重送'}
                      disabled={busy || !canSupplement}
                      onClick={() => void runAction(
                        `${ENDPOINT}/envelopes/${detail.id}/supplement`,
                        { operator: '值班管理员' },
                        '要求补证',
                      )}
                    >
                      凭原信封要求补证
                    </button>
                    <button
                      className="btn ghost"
                      type="button"
                      disabled={busy}
                      onClick={() => void runAction(
                        `${ENDPOINT}/batches/${detail.batch_id}/append`,
                        { rows: 2 },
                        '追加落盘内容',
                      )}
                    >
                      演练：向落盘批次追加内容
                    </button>
                  </div>
                </div>
                {detail.actions.length > 0 && (
                  <ul className="action-log">
                    {detail.actions.map((entry, index) => (
                      <li key={index}>
                        <span className="mono">{formatTime(entry.at)}</span>
                        <span>{entry.by}</span>
                        <strong>{entry.action}</strong>
                        <span className="muted">{entry.detail}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </section>

              <form
                className="receipt-form"
                onSubmit={(event) => {
                  event.preventDefault()
                  void runAction(`${ENDPOINT}/envelopes/${detail.id}/receipts`, {
                    stage: receiptStage,
                    verdict: receiptVerdict,
                    note: receiptNote,
                    source: '人工补录',
                    operator: '值班管理员',
                    channel_identity: receiptChannel || null,
                    attempt_no: detail.attempts.length > 0 ? detail.attempts[detail.attempts.length - 1].attempt_no : null,
                  }, '补录回执')
                }}
              >
                <div className="section-title">补录接收端回执</div>
                <div className="receipt-form-row">
                  <label className="filter-item">
                    <span>证明阶段</span>
                    <select value={receiptStage} onChange={(event) => setReceiptStage(event.target.value as Stage)}>
                      {(options?.stages ?? ['送达证明', '完整性验证', '接收确认']).map((stage) => (
                        <option key={stage} value={stage}>{stage}</option>
                      ))}
                    </select>
                  </label>
                  <label className="filter-item">
                    <span>结论</span>
                    <select value={receiptVerdict} onChange={(event) => setReceiptVerdict(event.target.value)}>
                      {stageVerdicts.map((verdict) => (
                        <option key={verdict} value={verdict}>{verdict}</option>
                      ))}
                    </select>
                  </label>
                  <label className="filter-item">
                    <span>回执归属通道身份</span>
                    <select value={receiptChannel} onChange={(event) => setReceiptChannel(event.target.value)}>
                      <option value="">本通道（{detail.channel_identity}）</option>
                      {channels
                        .filter((channel) => channel['通道身份'] !== detail.channel_identity)
                        .map((channel) => (
                          <option key={channel.id} value={channel['通道身份']}>
                            {channel['通道身份']}（跨通道，不抵扣）
                          </option>
                        ))}
                    </select>
                  </label>
                  <label className="filter-item grow">
                    <span>说明</span>
                    <input value={receiptNote} onChange={(event) => setReceiptNote(event.target.value)} placeholder="回执来源与结论依据" />
                  </label>
                  <button className="btn" type="submit" disabled={busy}>补录回执</button>
                </div>
              </form>
            </>
          )}
        </main>
      </div>
    </section>
  )
}
