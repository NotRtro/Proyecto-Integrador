import { useEffect, useState } from 'react'
import { api } from './api'

export function useAsync(loader, dependencies = []) {
  const [state, setState] = useState({ data: null, loading: true, error: null })
  useEffect(() => {
    let active = true
    setState(previous => ({ ...previous, loading: true, error: null }))
    loader().then(data => active && setState({ data, loading: false, error: null }))
      .catch(error => active && setState({ data: null, loading: false, error }))
    return () => { active = false }
  }, dependencies) // eslint-disable-line react-hooks/exhaustive-deps
  return state
}

// /api/meta se pide una sola vez y se comparte entre páginas.
let metaPromise = null
export function useMeta() {
  return useAsync(() => {
    metaPromise = metaPromise || api('/api/meta').catch(error => { metaPromise = null; throw error })
    return metaPromise
  }, [])
}
