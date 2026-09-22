import { useEffect, useState } from 'react'

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
