import { useCallback, useEffect, useState } from 'react'
import type { Subscription, SubscriptionPayload, SubscriptionStatus } from '../types'
import {
  activateSubscription,
  createSubscription,
  deleteSubscription,
  fetchSubscriptionStatus,
  listSubscriptions,
  refreshOneSubscription,
  refreshSubscription,
  toggleSubscription,
  updateSubscription,
} from '../api/client'

export default function SubscriptionsPanel() {
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([])
  const [status, setStatus] = useState<SubscriptionStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [action, setAction] = useState<string | null>(null)
  const [error, setError] = useState('')
  const [success, setSuccess] = useState('')
  const [newName, setNewName] = useState('')
  const [newUrl, setNewUrl] = useState('')
  const [editing, setEditing] = useState<Subscription | null>(null)
  const [editName, setEditName] = useState('')
  const [editUrl, setEditUrl] = useState('')
  const [deleteTarget, setDeleteTarget] = useState<Subscription | null>(null)

  const load = useCallback(async () => {
    const [list, currentStatus] = await Promise.all([listSubscriptions(), fetchSubscriptionStatus()])
    setSubscriptions(list.subscriptions || [])
    setStatus(currentStatus)
  }, [])

  useEffect(() => {
    const initialize = async () => {
      try {
        await load()
      } catch (err) {
        setError(err instanceof Error ? err.message : '加载订阅失败')
      } finally {
        setLoading(false)
      }
    }
    void initialize()
  }, [load])

  useEffect(() => {
    if (!success) return
    const timer = setTimeout(() => setSuccess(''), 5000)
    return () => clearTimeout(timer)
  }, [success])

  const payloadFor = (name: string, url: string, current?: Subscription): SubscriptionPayload => ({
    name: name.trim(), url: url.trim(), enabled: current?.enabled ?? true,
    refresh_interval_seconds: current?.refresh_interval_seconds ?? 0,
    refresh_timeout_seconds: current?.refresh_timeout_seconds ?? 0,
    sort_order: current?.sort_order ?? subscriptions.length,
  })

  const runAction = async (key: string, operation: () => Promise<unknown>, message: string) => {
    setAction(key)
    setError('')
    setSuccess('')
    try {
      await operation()
      await load()
      setSuccess(message)
      return true
    } catch (err) {
      setError(err instanceof Error ? err.message : '订阅操作失败')
      return false
    } finally {
      setAction(null)
    }
  }

  const addSubscription = async () => {
    if (!newName.trim() || !newUrl.trim()) return
    if (await runAction('create', () => createSubscription(payloadFor(newName, newUrl)), '订阅已添加')) {
      setNewName('')
      setNewUrl('')
    }
  }

  const startEditing = (subscription: Subscription) => {
    setEditing(subscription)
    setEditName(subscription.name)
    setEditUrl(subscription.url)
  }

  const saveSubscription = async () => {
    if (!editing || !editName.trim() || !editUrl.trim()) return
    if (await runAction(`edit-${editing.id}`, () => updateSubscription(editing.id, payloadFor(editName, editUrl, editing)), '订阅已更新')) setEditing(null)
  }

  const enabledCount = subscriptions.filter((subscription) => subscription.enabled).length
  const nodeCount = subscriptions.reduce((total, subscription) => total + subscription.node_count, 0)
  const errorCount = subscriptions.filter((subscription) => subscription.last_error).length
  const busy = action !== null || status?.is_refreshing

  if (loading) return <div className="flex h-64 items-center justify-center"><span className="loading loading-spinner loading-lg text-primary" /></div>

  return (
    <div className="min-h-full animate-in fade-in duration-500">
      <div className="border-b border-base-300/60 bg-base-100/80 px-4 py-5 backdrop-blur-xl lg:px-8">
        <div className="mx-auto flex w-full max-w-[1200px] flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-2xl font-bold">订阅管理</h2>
            <p className="mt-1 text-sm text-base-content/50">集中管理订阅源、同步状态与运行时节点</p>
          </div>
          <button className="btn btn-primary gap-2" disabled={busy || subscriptions.length === 0} onClick={() => void runAction('refresh-all', refreshSubscription, '全部订阅刷新完成')}>
            {action === 'refresh-all' || status?.is_refreshing ? <span className="loading loading-spinner loading-sm" /> : null}
            全量刷新
          </button>
        </div>
      </div>

      <div className="mx-auto w-full max-w-[1200px] space-y-6 p-4 pb-10 lg:p-8">
        {error && <div role="alert" className="alert alert-error alert-soft"><span>{error}</span></div>}
        {success && <div role="alert" className="alert alert-success alert-soft"><span>{success}</span></div>}
        {status?.last_error && <div role="alert" className="alert alert-warning alert-soft"><span>最近同步错误：{status.last_error}</span></div>}

        <div className="stats stats-vertical w-full border border-base-300/60 bg-base-100 shadow-sm sm:stats-horizontal">
          <div className="stat"><div className="stat-title">订阅总数</div><div className="stat-value text-primary">{subscriptions.length}</div></div>
          <div className="stat"><div className="stat-title">已启用</div><div className="stat-value text-success">{enabledCount}</div></div>
          <div className="stat"><div className="stat-title">节点总数</div><div className="stat-value">{status?.node_count ?? nodeCount}</div></div>
          <div className="stat"><div className="stat-title">异常订阅</div><div className={`stat-value ${errorCount ? 'text-error' : 'text-base-content'}`}>{errorCount}</div></div>
        </div>

        <section className="rounded-2xl border border-base-300/50 bg-base-100 p-5 shadow-sm lg:p-8">
          <div className="mb-5 flex flex-wrap items-center justify-between gap-3 border-b border-base-200 pb-4">
            <div><h3 className="text-lg font-bold">订阅源</h3><p className="text-xs text-base-content/50">变更会立即同步到运行时</p></div>
            <span className={`badge ${status?.enabled ? 'badge-success' : 'badge-ghost'}`}>{status?.enabled ? '自动刷新已开启' : '自动刷新未开启'}</span>
          </div>
          <div className="grid gap-3 sm:grid-cols-[minmax(0,0.7fr)_minmax(0,1.3fr)_auto]">
            <input className="input w-full bg-base-200/50" placeholder="订阅名称" value={newName} onChange={(event) => setNewName(event.target.value)} />
            <input type="url" className="input w-full bg-base-200/50 font-mono text-sm" placeholder="https://example.com/subscribe" value={newUrl} onChange={(event) => setNewUrl(event.target.value)} onKeyDown={(event) => event.key === 'Enter' && void addSubscription()} />
            <button className="btn btn-primary" disabled={!newName.trim() || !newUrl.trim() || busy} onClick={() => void addSubscription()}>{action === 'create' && <span className="loading loading-spinner loading-sm" />}添加</button>
          </div>

          {subscriptions.length ? <div className="mt-5 space-y-3">{subscriptions.map((subscription) => (
            <article key={subscription.id} className={`rounded-xl border bg-base-200/30 p-4 ${subscription.enabled ? 'border-base-300' : 'border-base-200 opacity-70'}`}>
              {editing?.id === subscription.id ? (
                <div className="grid gap-3 sm:grid-cols-[minmax(0,0.7fr)_minmax(0,1.3fr)_auto]">
                  <input className="input input-sm w-full" value={editName} onChange={(event) => setEditName(event.target.value)} />
                  <input className="input input-sm w-full font-mono text-xs" value={editUrl} onChange={(event) => setEditUrl(event.target.value)} />
                  <div className="flex gap-2"><button className="btn btn-primary btn-sm" disabled={!editName.trim() || !editUrl.trim() || busy} onClick={() => void saveSubscription()}>保存</button><button className="btn btn-ghost btn-sm" disabled={busy} onClick={() => setEditing(null)}>取消</button></div>
                </div>
              ) : (
                <div className="flex flex-col gap-4 lg:flex-row lg:items-center">
                  <div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-2"><strong>{subscription.name}</strong><span className={`badge badge-sm ${subscription.enabled ? 'badge-success' : 'badge-ghost'}`}>{subscription.enabled ? '已启用' : '已禁用'}</span><span className="badge badge-outline badge-sm">{subscription.node_count} 节点</span></div><code className="mt-1 block break-all text-xs text-base-content/55">{subscription.url}</code><div className="mt-2 flex flex-wrap gap-x-4 text-xs text-base-content/55"><span>最近成功：{subscription.last_success && !subscription.last_success.startsWith('0001-') ? new Date(subscription.last_success).toLocaleString() : '尚未成功'}</span>{subscription.last_error && <span className="break-all text-error">错误：{subscription.last_error}</span>}</div></div>
                  <div className="flex flex-wrap gap-2 lg:justify-end"><button className="btn btn-ghost btn-sm" disabled={busy} onClick={() => startEditing(subscription)}>编辑</button><button className="btn btn-ghost btn-sm" disabled={busy} onClick={() => void runAction(`toggle-${subscription.id}`, () => toggleSubscription(subscription.id, !subscription.enabled), subscription.enabled ? '订阅已禁用' : '订阅已启用')}>{action === `toggle-${subscription.id}` && <span className="loading loading-spinner loading-xs" />}{subscription.enabled ? '禁用' : '启用'}</button><button className="btn btn-ghost btn-sm" disabled={busy} onClick={() => void runAction(`refresh-${subscription.id}`, () => refreshOneSubscription(subscription.id), `${subscription.name} 刷新完成`)}>{action === `refresh-${subscription.id}` && <span className="loading loading-spinner loading-xs" />}刷新</button><button className="btn btn-ghost btn-sm text-primary" disabled={busy || (subscription.enabled && enabledCount === 1)} onClick={() => void runAction(`activate-${subscription.id}`, () => activateSubscription(subscription.id), `已独占启用 ${subscription.name}`)}>独占启用</button><button className="btn btn-ghost btn-sm text-error" disabled={busy} onClick={() => setDeleteTarget(subscription)}>删除</button></div>
                </div>
              )}
              {deleteTarget?.id === subscription.id && <div className="alert alert-warning mt-3 flex-col items-start sm:flex-row sm:items-center"><span>确认删除订阅“{subscription.name}”？此操作会同步更新运行时节点。</span><div className="flex gap-2 sm:ml-auto"><button className="btn btn-error btn-sm" disabled={busy} onClick={() => void runAction(`delete-${subscription.id}`, () => deleteSubscription(subscription.id), '订阅已删除').then((deleted) => deleted && setDeleteTarget(null))}>确认删除</button><button className="btn btn-ghost btn-sm" disabled={busy} onClick={() => setDeleteTarget(null)}>取消</button></div></div>}
            </article>
          ))}</div> : <div className="mt-5 rounded-xl border border-dashed border-base-300 bg-base-200/20 px-4 py-12 text-center"><p className="font-medium text-base-content/60">暂无订阅链接</p><p className="mt-1 text-sm text-base-content/40">在上方添加节点订阅地址</p></div>}
        </section>
      </div>
    </div>
  )
}
