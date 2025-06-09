import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"
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

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title} (Last {timeRange})</CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 12 }}
              interval="preserveStartEnd"
              minTickGap={20}
            />
            <YAxis allowDecimals={false} />
            <Tooltip />
            <Line
              type="monotone"
              dataKey="events"
              stroke="#8884d8"
              strokeWidth={2}
              dot={{ fill: "#8884d8" }}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
