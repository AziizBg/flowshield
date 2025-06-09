import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart } from "recharts"
import type { Event } from "@/lib/mock-data"

interface TrendChartProps {
  events: Event[]
  title?: string
  timeRange?: "12h" | "6h" | "5h" | "3h" | "2h" | "1h" | "30m" | "1m"
}

export function TrendChart({ events, title = "Event Trends", timeRange = "1h" }: TrendChartProps) {
  // Calculate time intervals based on selected range
  const getTimeIntervals = () => {
    const now = new Date()
    const intervals = []
    let intervalMinutes = 0
    let numIntervals = 0

    switch (timeRange) {
      case "12h":
        intervalMinutes = 60 // 1 hour intervals
        numIntervals = 12
        break
      case "6h":
        intervalMinutes = 30 // 30 minute intervals
        numIntervals = 12
        break
      case "5h":
        intervalMinutes = 25 // 25 minute intervals
        numIntervals = 12
        break
      case "3h":
        intervalMinutes = 15 // 15 minute intervals
        numIntervals = 12
        break
      case "2h":
        intervalMinutes = 10 // 10 minute intervals
        numIntervals = 12
        break
      case "1h":
        intervalMinutes = 5 // 5 minute intervals
        numIntervals = 12
        break
      case "30m":
        intervalMinutes = 2.5 // 2.5 minute intervals
        numIntervals = 12
        break
      case "1m":
        intervalMinutes = 5 // 5 second intervals
        numIntervals = 12
        break
      default:
        intervalMinutes = 5
        numIntervals = 12
    }

    // Create intervals from now going backwards
    for (let i = 0; i < numIntervals; i++) {
      const time = new Date(now.getTime() - (numIntervals - 1 - i) * intervalMinutes * 60 * 1000)
      intervals.push({
        time: time.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" }),
        events: 0,
        startTime: time.getTime(),
        endTime: new Date(time.getTime() + intervalMinutes * 60 * 1000).getTime()
      })
    }

    return intervals
  }

  const timeIntervals = getTimeIntervals()

  // Count events for each interval
  events.forEach((event) => {
    const eventTime = new Date(event.time).getTime()

    // Find the interval that contains this event
    const intervalIndex = timeIntervals.findIndex(interval =>
      eventTime >= interval.startTime && eventTime < interval.endTime
    )

    if (intervalIndex !== -1) {
      timeIntervals[intervalIndex].events++
    }
  })

  // Remove the internal time properties before rendering
  const chartData = timeIntervals.map(({ time, events }) => ({ time, events }))

  // Find the maximum number of events for color scaling
  const maxEvents = Math.max(...chartData.map(d => d.events))

  // Custom tooltip component
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-white p-3 rounded-lg shadow-lg border border-gray-100">
          <p className="text-sm font-medium text-gray-900">{label}</p>
          <p className="text-sm text-red-600">
            {payload[0].value} {payload[0].value === 1 ? 'event' : 'events'}
          </p>
        </div>
      )
    }
    return null
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg font-semibold text-gray-900">{title} (Last {timeRange})</CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="colorEvents" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid
              strokeDasharray="3 3"
              vertical={false}
              stroke="#f0f0f0"
            />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 12, fill: '#6b7280' }}
              tickLine={false}
              axisLine={{ stroke: '#e5e7eb' }}
              interval="preserveStartEnd"
              minTickGap={20}
            />
            <YAxis
              allowDecimals={false}
              tick={{ fontSize: 12, fill: '#6b7280' }}
              tickLine={false}
              axisLine={{ stroke: '#e5e7eb' }}
            />
            <Tooltip content={<CustomTooltip />} />
            <Area
              type="monotone"
              dataKey="events"
              stroke="#ef4444"
              strokeWidth={2}
              fillOpacity={1}
              fill="url(#colorEvents)"
              isAnimationActive={false}
              dot={({ cx, cy, payload }) => {
                const intensity = payload.events / maxEvents;
                return (
                  <circle
                    cx={cx}
                    cy={cy}
                    r={4}
                    fill={intensity > 0.7 ? "#ef4444" : "#fca5a5"}
                    stroke="#ef4444"
                    strokeWidth={2}
                  />
                );
              }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
