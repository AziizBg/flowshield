"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Zap, Flame, Clock, MapPin } from "lucide-react"
import type { Event } from "@/lib/mock-data"
import Link from "next/link"
import { useState, useEffect } from "react"

interface EventFeedProps {
  events: Event[]
  showAll?: boolean
  onEventClick?: (event: Event) => void
}

export function EventFeed({ events, onEventClick }: EventFeedProps) {
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null)

  const getSeverityVariant = (severity: string) => {
    switch (severity) {
      case "high":
        return "destructive"
      case "moderate":
        return "default"
      case "low":
        return "secondary"
      default:
        return "outline"
    }
  }

  const formatTime = (time: string) => {
    const date = new Date(time)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffMins = Math.floor(diffMs / 60000)

    if (diffMins < 1) return "Just now"
    if (diffMins < 60) return `${diffMins}m ago`
    if (diffMins < 1440) return `${Math.floor(diffMins / 60)}h ago`
    return date.toLocaleDateString()
  }

  const handleEventClick = (event: Event) => {
    setSelectedEventId(event.id)
    onEventClick?.(event)
  }

  // Reset selection when events change
  useEffect(() => {
    setSelectedEventId(null)
  }, [events])

  return (
    <div className="h-full flex flex-col">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-lg font-semibold text-gray-900">Live Event Feed</h2>
        <Badge variant="outline" className="text-sm">
          {events.length} events
        </Badge>
      </div>
      <ScrollArea className="flex-1 pr-4">
        <div className="space-y-3">
          {events.map((event) => (
            <div
              key={event.id}
              onClick={() => handleEventClick(event)}
              className={`p-4 bg-white rounded-lg shadow-sm border transition-all cursor-pointer group ${selectedEventId === event.id
                ? "border-blue-400 shadow-md bg-blue-50"
                : "border-gray-100 hover:shadow-md hover:border-blue-200"
                }`}
            >
              <div className="flex items-start justify-between">
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    <span className={`transition-colors ${selectedEventId === event.id
                      ? "text-blue-600"
                      : "text-gray-500 group-hover:text-blue-600"
                      }`}>
                      {event.type === "earthquake" ? (
                        <Zap className="h-4 w-4" />
                      ) : (
                        <Flame className="h-4 w-4" />
                      )}
                    </span>
                    <span className={`text-sm font-medium transition-colors ${selectedEventId === event.id
                      ? "text-blue-600"
                      : "text-gray-900 group-hover:text-blue-600"
                      }`}>
                      {event.type === "earthquake" ? "Earthquake" : "Fire"}
                    </span>
                    <Badge
                      variant={getSeverityVariant(event.severity)}
                      className="text-xs"
                    >
                      {event.severity.charAt(0).toUpperCase() + event.severity.slice(1)}
                    </Badge>
                  </div>
                  <p className="text-sm text-gray-600 flex items-center gap-1">
                    <MapPin className="h-3 w-3" />
                    {event.place}
                  </p>
                  <p className="text-xs text-gray-500 flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    {formatTime(event.time)}
                  </p>
                </div>
                {event.type === "earthquake" && (
                  <div className="text-right">
                    <span className="text-sm font-medium text-gray-900">
                      Magnitude {event.magnitude}
                    </span>
                  </div>
                )}
              </div>
            </div>
          ))}
          {events.length === 0 && (
            <div className="p-4 text-center text-gray-500 bg-gray-50 rounded-lg">
              No events to display
            </div>
          )}
        </div>
      </ScrollArea>
    </div>
  )
}
