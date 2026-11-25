export async function getJSON(url, signal) {
  const response = await fetch(url, { signal })
  if (!response.ok) throw Object.assign(new Error(`HTTP ${response.status}`), { status: response.status })
  return response.json()
}
