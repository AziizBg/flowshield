import { useState, useEffect } from "react"
import { fetchEarthquakeData, fetchFireData, generateMockEvent, type Event } from "@/lib/mock-data"

interface UseEventsReturn {
  events: Event[]
  filteredEvents: Event[]
  isLoading: boolean
  loadingStatus: string
  lastUpdated: Date | null
  error: string | null
  useRealData: boolean
  isRealTime: boolean
  setSelectedEventType: (type: "all" | "earthquake" | "fire") => void
  setSelectedSeverity: (severity: "all" | "low" | "moderate" | "high") => void
  setSelectedTimeRange: (range: "all" | "1h" | "2h" | "3h" | "5h" | "6h" | "12h" | "24h" | "30m" | "1m") => void
  setUseRealData: (useReal: boolean) => void
  setIsRealTime: (isReal: boolean) => void
  refreshData: () => Promise<void>
}

export function useEvents(
  selectedEventType: "all" | "earthquake" | "fire",
  selectedSeverity: "all" | "low" | "moderate" | "high",
  selectedTimeRange: "all" | "1h" | "2h" | "3h" | "5h" | "6h" | "12h" | "24h" | "30m" | "1m" = "1h"
): UseEventsReturn {
  const [events, setEvents] = useState<Event[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [useRealData, setUseRealData] = useState(true)
  const [isRealTime, setIsRealTime] = useState(true)
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [loadingStatus, setLoadingStatus] = useState<string>("")

  // Fetch data when filters change
  useEffect(() => {
    fetchRealData()
  }, [selectedEventType, selectedSeverity, selectedTimeRange])

  const fetchRealData = async () => {
    setLoading(true)
    setLoadingStatus("Initializing...")

    try {
      // Fetch both earthquake and fire data
      setLoadingStatus("Fetching earthquake data...")
      const earthquakeData = await fetchEarthquakeData()
      
      setLoadingStatus("Fetching fire data...")
      const fireData = await fetchFireData()

      setLoadingStatus("Processing data...")
      // Combine and sort the data
      const combinedData = [...earthquakeData, ...fireData].sort((a, b) => {
        return new Date(b.time).getTime() - new Date(a.time).getTime()
      })

      // Filter based on selected time range
      const now = new Date()
      const filteredData = combinedData.filter(event => {
        const eventTime = new Date(event.time)
        const timeDiff = now.getTime() - eventTime.getTime()
        
        switch (selectedTimeRange) {
          case "12h":
            return timeDiff <= 12 * 60 * 60 * 1000 // 12 hours
          case "6h":
            return timeDiff <= 6 * 60 * 60 * 1000 // 6 hours
          case "5h":
            return timeDiff <= 5 * 60 * 60 * 1000 // 5 hours
          case "3h":
            return timeDiff <= 3 * 60 * 60 * 1000 // 3 hours
          case "2h":
            return timeDiff <= 2 * 60 * 60 * 1000 // 2 hours
          case "1h":
            return timeDiff <= 60 * 60 * 1000 // 1 hour
          case "30m":
            return timeDiff <= 30 * 60 * 1000 // 30 minutes
          case "1m":
            return timeDiff <= 60 * 1000 // 1 minute
          default:
            return true // Show all events if no time range is selected
        }
      })

      setEvents(filteredData)
      setLastUpdated(new Date())
      setError(null)
      setLoadingStatus("Data updated successfully")
    } catch (err) {
      console.error("Error fetching data:", err)
      setError("Failed to fetch event data")
      setLoadingStatus("Error occurred while fetching data")
    } finally {
      setLoading(false)
      // Clear the status message after a short delay
      setTimeout(() => setLoadingStatus(""), 2000)
    }
  }

  // Filter events based on selected criteria
  const filteredEvents = events.filter(event => {
    const typeMatch = selectedEventType === "all" || event.type === selectedEventType
    const severityMatch = selectedSeverity === "all" || event.severity === selectedSeverity
    return typeMatch && severityMatch
  })

  return {
    events,
    filteredEvents,
    isLoading: loading,
    loadingStatus,
    lastUpdated,
    error,
    useRealData,
    isRealTime,
    setSelectedEventType: (type: "all" | "earthquake" | "fire") => {
      // This is handled by the parent component
    },
    setSelectedSeverity: (severity: "all" | "low" | "moderate" | "high") => {
      // This is handled by the parent component
    },
    setSelectedTimeRange: (range: "all" | "1h" | "2h" | "3h" | "5h" | "6h" | "12h" | "24h" | "30m" | "1m") => {
      // This is handled by the parent component
    },
    setUseRealData,
    setIsRealTime,
    refreshData: fetchRealData,
  }
} 