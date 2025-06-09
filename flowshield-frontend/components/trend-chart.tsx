import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"
import type { Event } from "@/lib/mock-data"

interface TrendChartProps {
  events: Event[]
  title?: string
}

export function TrendChart({ events, title = "Event Trends (Last 24 Hours)" }: TrendChartProps) {
  // Group events by hour for the last 24 hours
  const now = new Date()
  const last24Hours = Array.from({ length: 24 }, (_, i) => {
    const hour = new Date(now.getTime() - (23 - i) * 60 * 60 * 1000)
    return {
      hour: hour.getHours(),
      time: hour.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" }),
      events: 0,
    }
  })

  // Count events for each hour
  events.forEach((event) => {
    const eventTime = new Date(event.time)
    const hoursDiff = Math.floor((now.getTime() - eventTime.getTime()) / (1000 * 60 * 60))
    if (hoursDiff >= 0 && hoursDiff < 24) {
      const index = 23 - hoursDiff
      if (last24Hours[index]) {
        last24Hours[index].events++
      }
    }
  })

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={last24Hours}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" tick={{ fontSize: 12 }} interval="preserveStartEnd" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="events" stroke="#8884d8" strokeWidth={2} dot={{ fill: "#8884d8" }} />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
