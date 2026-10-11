// jsdom no implementa ResizeObserver, que usan los gráficos de recharts.
globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} }
globalThis.IS_REACT_ACT_ENVIRONMENT = true
window.scrollTo = () => {}
