import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Zap, Flame, TrendingUp, MapPin } from "lucide-react"
import type { Event } from "@/lib/mock-data"

interface MetricsCardsProps {
  earthquakeEvents: Event[]
  fireEvents: Event[]
  totalEvents: number
}

export function MetricsCards({ earthquakeEvents, fireEvents, totalEvents }: MetricsCardsProps) {
  const avgEarthquakeMagnitude =
    earthquakeEvents.length > 0
      ? (earthquakeEvents.reduce((sum, e) => sum + (e.magnitude || 0), 0) / earthquakeEvents.length).toFixed(1)
      : "0.0"

  const avgFireFRP =
    fireEvents.length > 0
      ? (fireEvents.reduce((sum, e) => sum + (e.frp || 0), 0) / fireEvents.length).toFixed(1)
      : "0.0"

  const highSeverityEvents =
    earthquakeEvents.filter((e) => e.severity === "high").length +
    fireEvents.filter((e) => e.severity === "high").length

  const recentEvents = [...earthquakeEvents, ...fireEvents].filter(
    (e) => Date.now() - new Date(e.time).getTime() < 3600000,
  ).length // Last hour

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Total Events</CardTitle>
          <TrendingUp className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{totalEvents}</div>
          <p className="text-xs text-muted-foreground">
            {earthquakeEvents.length} earthquakes, {fireEvents.length} fires
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Avg Earthquake Magnitude</CardTitle>
          <Zap className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{avgEarthquakeMagnitude}</div>
          <p className="text-xs text-muted-foreground">Richter scale</p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">Avg Fire FRP</CardTitle>
          <Flame className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{avgFireFRP}</div>
          <p className="text-xs text-muted-foreground">MW (Megawatts)</p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-medium">High Severity Events</CardTitle>
          <MapPin className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{highSeverityEvents}</div>
          <p className="text-xs text-muted-foreground">{recentEvents} in last hour</p>
        </CardContent>
      </Card>
    </div>
  )
}
