const currency = new Intl.NumberFormat('ru-RU', { style: 'currency', currency: 'RUB' })
const timestamp = new Intl.DateTimeFormat('ru-RU', {
  dateStyle: 'medium',
  timeStyle: 'short',
  timeZone: 'UTC',
})
const calendarDate = new Intl.DateTimeFormat('ru-RU', { dateStyle: 'long', timeZone: 'UTC' })

export function formatAmount(kopecks) {
  return currency.format(kopecks / 100)
}

export function formatTime(time) {
  return timestamp.format(new Date(time))
}

export function formatFeatureDate(date) {
  return calendarDate.format(new Date(`${date}T00:00:00Z`))
}
