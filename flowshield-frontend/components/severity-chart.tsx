import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts"
import type { Event } from "@/lib/mock-data"

interface SeverityChartProps {
  events: Event[]
  title?: string
}

export function SeverityChart({ events, title = "Event Severity Distribution" }: SeverityChartProps) {
  const severityData = [
    {
      name: "High",
      value: events.filter((e) => e.severity === "high").length,
      color: "#ef4444",
    },
    {
      name: "Moderate",
      value: events.filter((e) => e.severity === "moderate").length,
      color: "#eab308",
    },
    {
      name: "Low",
      value: events.filter((e) => e.severity === "low").length,
      color: "#22c55e",
    },
  ]

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <PieChart>
            <Pie
              data={severityData}
              cx="50%"
              cy="50%"
              labelLine={false}
              label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
              outerRadius={80}
              fill="#8884d8"
              dataKey="value"
            >
              {severityData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
            <Tooltip />
            <Legend />
          </PieChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
