export interface Event {
  id: string
  type: "earthquake" | "fire"
  time: string
  latitude: number
  longitude: number
  place: string
  severity: "low" | "moderate" | "high"
  magnitude?: number
  frp?: number
  city?: string
  country?: string
}

// Simple country/region mapping based on coordinates
function getRegionFromCoordinates(lat: number, lng: number): string {
  // North America
  if (lat >= 25 && lat <= 70 && lng >= -170 && lng <= -50) {
    if (lng >= -130 && lng <= -60) return "North America"
    if (lng >= -170 && lng <= -130) return "Alaska/Western Canada"
  }

  // South America
  if (lat >= -60 && lat <= 15 && lng >= -85 && lng <= -30) {
    return "South America"
  }

  // Europe
  if (lat >= 35 && lat <= 75 && lng >= -15 && lng <= 50) {
    return "Europe"
  }

  // Asia
  if (lat >= -10 && lat <= 80 && lng >= 50 && lng <= 180) {
    if (lat >= 50 && lng >= 80) return "Siberia"
    if (lat >= 20 && lng >= 100 && lng <= 140) return "East Asia"
    if (lat >= 0 && lng >= 90 && lng <= 150) return "Southeast Asia"
    return "Asia"
  }

  // Africa
  if (lat >= -40 && lat <= 40 && lng >= -20 && lng <= 55) {
    return "Africa"
  }

  // Australia/Oceania
  if (lat >= -50 && lat <= -5 && lng >= 110 && lng <= 180) {
    return "Australia/Oceania"
  }

  // Default fallback
  return `${lat.toFixed(1)}°, ${lng.toFixed(1)}°`
}

// Real API polling functions
export async function fetchEarthquakeData(): Promise<Event[]> {
  try {
    // Use "all_day" instead of "all_hour" to get more earthquake events
    const response = await fetch("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson")
    const data = await response.json()

    return data.features.map((feature: any) => {
      const magnitude = feature.properties.mag || 0
      let severity: "low" | "moderate" | "high"

      if (magnitude < 4.0) severity = "low"
      else if (magnitude < 6.0) severity = "moderate"
      else severity = "high"

      return {
        id: feature.id,
        type: "earthquake" as const,
        time: new Date(feature.properties.time).toISOString(),
        latitude: feature.geometry.coordinates[1],
        longitude: feature.geometry.coordinates[0],
        place: feature.properties.place || "Unknown location",
        severity,
        magnitude: Number(magnitude.toFixed(1)),
        city: extractCityFromPlace(feature.properties.place),
        country: extractCountryFromPlace(feature.properties.place),
      }
    })
  } catch (error) {
    console.error("Error fetching earthquake data:", error)
    return []
  }
}

export async function fetchFireData(): Promise<Event[]> {
  try {
    const today = new Date().toISOString().split("T")[0]
    const url = `https://firms.modaps.eosdis.nasa.gov/api/area/csv/477785c4a607ad274bbbb9cbdcdd6bef/VIIRS_SNPP_NRT/world/1/${today}`

    console.log("Fetching fire data from:", url)
    const response = await fetch(url)
    const csvText = await response.text()
    console.log("Fire data CSV response:", csvText.substring(0, 500)) // Log first 500 chars

    // Parse CSV data
    const lines = csvText.split("\n")
    const headers = lines[0].split(",")
    console.log("Fire data headers:", headers)
    const events: Event[] = []

    // Process all lines, removing the 500 limit
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim()
      if (!line) continue

      const values = line.split(",")
      const data: any = {}

      headers.forEach((header, index) => {
        data[header.trim()] = values[index]?.trim()
      })

      if (data.latitude && data.longitude && data.frp) {
        const frp = Number.parseFloat(data.frp) || 0
        let severity: "low" | "moderate" | "high"

        if (frp < 5) severity = "low"
        else if (frp < 15) severity = "moderate"
        else severity = "high"

        const lat = Number.parseFloat(data.latitude)
        const lng = Number.parseFloat(data.longitude)

        // Create datetime from acq_date and acq_time
        let dateTime: string
        try {
          const timeStr = data.acq_time?.padStart(4, "0") || "0000"
          const hours = timeStr.slice(0, 2)
          const minutes = timeStr.slice(2, 4)
          dateTime = `${data.acq_date}T${hours}:${minutes}:00Z`
        } catch {
          dateTime = `${data.acq_date}T00:00:00Z`
        }

        // Use simple region mapping instead of API calls
        const region = getRegionFromCoordinates(lat, lng)

        const event = {
          id: `fire_${data.latitude}_${data.longitude}_${data.acq_date}_${data.acq_time}`,
          type: "fire" as const,
          time: new Date(dateTime).toISOString(),
          latitude: lat,
          longitude: lng,
          place: region,
          severity,
          frp: Number(frp.toFixed(1)),
          city: region.split("/")[0], // Use first part of region as city
          country: region,
        }
        console.log("Created fire event:", event)
        events.push(event)
      }
    }

    console.log("Total fire events created:", events.length)
    return events
  } catch (error) {
    console.error("Error fetching fire data:", error)
    return []
  }
}

// Helper functions
function extractCityFromPlace(place: string): string {
  if (!place) return "Unknown"
  // Extract city from USGS place format (e.g., "5km NE of City, State")
  const parts = place.split(" of ")
  if (parts.length > 1) {
    const cityPart = parts[1].split(",")[0]
    return cityPart.trim()
  }
  return place.split(",")[0].trim()
}

function extractCountryFromPlace(place: string): string {
  if (!place) return "Unknown"
  const parts = place.split(",")
  return parts[parts.length - 1].trim()
}

// Keep the mock function as fallback
export function generateMockEvent(): Event {
  const isEarthquake = Math.random() > 0.4 // 60% earthquakes, 40% fires
  const eventType = isEarthquake ? "earthquake" : "fire"

  const cities = [
    { name: "San Francisco", country: "USA", lat: 37.7749, lng: -122.4194 },
    { name: "Tokyo", country: "Japan", lat: 35.6762, lng: 139.6503 },
    { name: "Los Angeles", country: "USA", lat: 34.0522, lng: -118.2437 },
    { name: "Mexico City", country: "Mexico", lat: 19.4326, lng: -99.1332 },
    { name: "Istanbul", country: "Turkey", lat: 41.0082, lng: 28.9784 },
    { name: "Jakarta", country: "Indonesia", lat: -6.2088, lng: 106.8456 },
    { name: "Manila", country: "Philippines", lat: 14.5995, lng: 120.9842 },
    { name: "Santiago", country: "Chile", lat: -33.4489, lng: -70.6693 },
    { name: "Anchorage", country: "USA", lat: 61.2181, lng: -149.9003 },
    { name: "Athens", country: "Greece", lat: 37.9838, lng: 23.7275 },
    { name: "Sydney", country: "Australia", lat: -33.8688, lng: 151.2093 },
    { name: "Rome", country: "Italy", lat: 41.9028, lng: 12.4964 },
    { name: "Lisbon", country: "Portugal", lat: 38.7223, lng: -9.1393 },
    { name: "Reykjavik", country: "Iceland", lat: 64.1466, lng: -21.9426 },
    { name: "Wellington", country: "New Zealand", lat: -41.2865, lng: 174.7762 },
  ]

  const fireLocations = [
    { name: "California Wildlands", country: "USA", lat: 36.7783, lng: -119.4179 },
    { name: "Amazon Rainforest", country: "Brazil", lat: -3.4653, lng: -62.2159 },
    { name: "Australian Outback", country: "Australia", lat: -25.2744, lng: 133.7751 },
    { name: "Siberian Forest", country: "Russia", lat: 60.0, lng: 100.0 },
    { name: "Canadian Boreal", country: "Canada", lat: 54.0, lng: -105.0 },
    { name: "Mediterranean Coast", country: "Spain", lat: 40.4637, lng: 3.7492 },
    { name: "Greek Islands", country: "Greece", lat: 37.9755, lng: 23.7348 },
    { name: "Portuguese Hills", country: "Portugal", lat: 39.3999, lng: -8.2245 },
  ]

  const locations = isEarthquake ? cities : fireLocations
  const location = locations[Math.floor(Math.random() * locations.length)]

  // Add some random variation to coordinates
  const lat = location.lat + (Math.random() - 0.5) * 2
  const lng = location.lng + (Math.random() - 0.5) * 2

  let severity: "low" | "moderate" | "high"
  let magnitude: number | undefined
  let frp: number | undefined

  if (isEarthquake) {
    magnitude = Math.random() * 8 + 1 // 1.0 to 9.0
    if (magnitude < 4.0) severity = "low"
    else if (magnitude < 6.0) severity = "moderate"
    else severity = "high"
  } else {
    frp = Math.random() * 25 // 0 to 25 MW
    if (frp < 5) severity = "low"
    else if (frp < 15) severity = "moderate"
    else severity = "high"
  }

  // Generate time within last 24 hours
  const now = new Date()
  const timeOffset = Math.random() * 24 * 60 * 60 * 1000 // Random time in last 24 hours
  const eventTime = new Date(now.getTime() - timeOffset)

  return {
    id: `${eventType}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    type: eventType,
    time: eventTime.toISOString(),
    latitude: lat,
    longitude: lng,
    place: `${location.name}, ${location.country}`,
    severity,
    magnitude: isEarthquake ? Number(magnitude!.toFixed(1)) : undefined,
    frp: !isEarthquake ? Number(frp!.toFixed(1)) : undefined,
    city: location.name,
    country: location.country,
  }
}
