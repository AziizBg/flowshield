"use client"

import { useState } from "react"
import { Card, CardContent } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Zap, Clock, Search as SearchIcon } from "lucide-react"
import { EventMap } from "@/components/event-map"
import { EventFeed } from "@/components/event-feed"
import { SeverityChart } from "@/components/severity-chart"
import { TrendChart } from "@/components/trend-chart"
import { MetricsCards } from "@/components/metrics-cards"
import { Header } from "./components/dashboard/Header"
import { Filters } from "./components/dashboard/Filters"
import { Search } from "./components/dashboard/Search"
import { Loading } from "./components/loading"
import { ErrorBoundary } from "./components/error-boundary"
import { useEvents } from "./hooks/useEvents"
import type { Event } from "@/lib/mock-data"
import Link from "next/link"

export default function StreamDashboard() {
  const [selectedEventType, setSelectedEventType] = useState<"all" | "earthquake" | "fire">("all")
  const [selectedSeverity, setSelectedSeverity] = useState<"all" | "low" | "moderate" | "high">("all")
  const [selectedTimeRange, setSelectedTimeRange] = useState<"12h" | "6h" | "5h" | "3h" | "2h" | "1h" | "30m" | "1m">("1h")
  const [searchQuery, setSearchQuery] = useState("")
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null)

  const { events, filteredEvents, isLoading, loadingStatus, lastUpdated, error } = useEvents(
    selectedEventType,
    selectedSeverity,
    selectedTimeRange
  )

  // Filter events based on search query
  const searchFilteredEvents = filteredEvents.filter(event => {
    const searchLower = searchQuery.toLowerCase()
    return (
      event.place?.toLowerCase().includes(searchLower) ||
      event.type.toLowerCase().includes(searchLower)
    )
  })

  const earthquakeEvents = events.filter((e: Event) => e.type === "earthquake")
  const fireEvents = events.filter((e: Event) => e.type === "fire")

  const handleEventSelect = (eventId: string | null) => {
    setSelectedEventId(eventId)
  }

  const handleEventClick = (event: Event) => {
    setSelectedEventId(event.id)
  }

  const handleEventTypeChange = (eventType: "all" | "earthquake" | "fire") => {
    setSelectedEventType(eventType)
  }

  const handleSeverityChange = (severity: "all" | "low" | "moderate" | "high") => {
    setSelectedSeverity(severity)
  }

  const handleTimeRangeChange = (timeRange: "12h" | "6h" | "5h" | "3h" | "2h" | "1h" | "30m" | "1m") => {
    setSelectedTimeRange(timeRange)
  }

  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-gray-50">
        <div className="container mx-auto px-4 py-6 space-y-6">
          <Header
            lastUpdated={new Date()}
            useRealData={true}
            isRealTime={true}
            isLoading={isLoading}
            onToggleDataMode={() => { }}
            onToggleRealTime={() => { }}
            onRefresh={() => { }}
            historicalDataLink={
              <Link
                href="/batch"
                className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-50 hover:bg-gray-100 text-gray-500 rounded-full transition-colors"
              >
                <Clock className="h-4 w-4" />
                <span className="text-sm font-medium">Historical</span>
              </Link>
            }
          />

          <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
            {/* Left sidebar with filters - reduced width */}
            <div className="lg:col-span-2">
              <Filters
                selectedEventType={selectedEventType}
                selectedSeverity={selectedSeverity}
                selectedTimeRange={selectedTimeRange}
                onEventTypeChange={handleEventTypeChange}
                onSeverityChange={handleSeverityChange}
                onTimeRangeChange={handleTimeRangeChange}
              />
            </div>

            {/* Main content area - increased width */}
            <div className="lg:col-span-10 space-y-6">
              {/* Search bar */}
              <div className="relative">
                <input
                  type="text"
                  placeholder="Search live events..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full px-4 py-2 pl-10 bg-white rounded-lg border border-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all"
                />
                <SearchIcon className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
              </div>

              {/* Loading and status indicators */}
              <div className="flex items-center justify-between bg-white rounded-lg p-4 shadow-sm border border-gray-100">
                {isLoading ? (
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
                    <span className="text-sm text-gray-600">{loadingStatus || "Loading live data..."}</span>
                  </div>
                ) : (
                  <div className="flex items-center gap-2 text-sm text-gray-600">
                    <Zap className="h-4 w-4 text-green-500" />
                    <span>Live data - Last updated: {lastUpdated?.toLocaleString()}</span>
                  </div>
                )}
                {error && (
                  <div className="text-sm text-red-600">
                    {error}
                  </div>
                )}
              </div>

              {/* Metrics Cards */}
              <MetricsCards
                totalEvents={events.length}
                earthquakeEvents={earthquakeEvents}
                fireEvents={fireEvents}
              />

              {/* Charts */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <Card>
                  <CardContent className="p-6">
                    <SeverityChart events={events} />
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="p-6">
                    <TrendChart events={events} timeRange={selectedTimeRange} />
                  </CardContent>
                </Card>
              </div>

              {/* Map and event feed */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2 h-[600px]">
                  <EventMap
                    events={filteredEvents}
                    selectedEventId={selectedEventId}
                    onEventSelect={handleEventSelect}
                  />
                </div>
                <div className="lg:col-span-1 h-[600px] overflow-hidden">
                  <Card className="h-full">
                    <CardContent className="p-4 h-full">
                      <EventFeed
                        events={filteredEvents}
                        showAll={true}
                        onEventClick={handleEventClick}
                      />
                    </CardContent>
                  </Card>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </ErrorBoundary>
  )
}
