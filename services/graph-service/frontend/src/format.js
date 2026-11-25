const currency = new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB' })
const timestamp = new Intl.DateTimeFormat('ru-RU', {
  dateStyle: 'medium',
  timeStyle: 'short',
  timeZone: 'UTC',
})

export function formatAmount(kopecks) {
  return currency.format(kopecks / 100)
}

export function formatTime(time) {
  return timestamp.format(new Date(time))
}
