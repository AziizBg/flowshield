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
      color: "#fca5a5",
    },
    {
      name: "Moderate",
      value: events.filter((e) => e.severity === "moderate").length,
      color: "#fde047",
    },
    {
      name: "Low",
      value: events.filter((e) => e.severity === "low").length,
      color: "#86efac",
    },
  ]

  // Custom legend component
  const CustomLegend = ({ payload }: any) => {
    return (
      <div className="mt-4 space-y-2">
        <div className="font-medium text-sm">Severity Distribution</div>
        {payload.map((entry: any, index: number) => (
          <div key={`legend-${index}`} className="flex items-center space-x-2">
            <div
              className="w-3 h-3 rounded-full border"
              style={{
                backgroundColor: entry.color,
                borderColor: `${entry.color}80`
              }}
            />
            <span className="text-sm">{entry.value}</span>
            <span className="text-sm text-muted-foreground">
              ({entry.payload.value} events)
            </span>
          </div>
        ))}
      </div>
    )
  }

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
            <Legend content={<CustomLegend />} />
          </PieChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}
