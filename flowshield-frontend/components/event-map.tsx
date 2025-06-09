"use client"

import type React from "react"

import { useState, useEffect, useRef } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { MapPin, Zap, Flame, ZoomIn, ZoomOut, RotateCcw, Map, Satellite, Clock } from "lucide-react"
import type { Event } from "@/lib/mock-data"
import Mapbox from "mapbox-gl"
import { Marker, NavigationControl, Map as ReactMap, MapRef, Popup } from "react-map-gl/dist/esm"

interface EventMapProps {
  events: Event[]
  selectedEventId?: string | null
  onEventSelect?: (eventId: string | null) => void
}

interface MapState {
  zoom: number
  centerLat: number
  centerLng: number
}

type MapStyle = "street" | "satellite" | "terrain" | "dark"

export function EventMap({ events, selectedEventId, onEventSelect }: EventMapProps) {
  const [mapState, setMapState] = useState<MapState>({
    zoom: 2,
    centerLat: 20,
    centerLng: 0,
  })
  const [mapStyle, setMapStyle] = useState<MapStyle>("street")
  const [mapLoaded, setMapLoaded] = useState(false)
  const [mapError, setMapError] = useState<string | null>(null)
  const [hoveredEventId, setHoveredEventId] = useState<string | null>(null)
  const mapRef = useRef<MapRef>(null)
  const isDraggingRef = useRef(false)
  const lastMousePosRef = useRef({ x: 0, y: 0 })

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

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case "high":
        return "bg-red-100 text-red-700"
      case "moderate":
        return "bg-yellow-100 text-yellow-700"
      case "low":
        return "bg-green-100 text-green-700"
      default:
        return "bg-gray-100 text-gray-700"
    }
  }

  const getEventIcon = (type: string) => {
    return type === "earthquake" ? Zap : Flame
  }

  // Convert lat/lng to pixel coordinates based on current map state
  const getPixelPosition = (lat: number, lng: number) => {
    if (!mapRef.current) return { x: 0, y: 0 }

    const map = mapRef.current.getMap()
    const mapWidth = map.getCanvas().width
    const mapHeight = map.getCanvas().height

    // Web Mercator projection
    const scale = (Math.pow(2, mapState.zoom) * 256) / (2 * Math.PI)

    // Convert to radians
    const latRad = (lat * Math.PI) / 180
    const lngRad = (lng * Math.PI) / 180
    const centerLatRad = (mapState.centerLat * Math.PI) / 180
    const centerLngRad = (mapState.centerLng * Math.PI) / 180

    // Calculate pixel coordinates
    const x = mapWidth / 2 + (lngRad - centerLngRad) * scale
    const y =
      mapHeight / 2 -
      (Math.log(Math.tan(Math.PI / 4 + latRad / 2)) - Math.log(Math.tan(Math.PI / 4 + centerLatRad / 2))) * scale

    return { x, y }
  }

  // Group nearby events to avoid overcrowding
  const groupedEvents = events.reduce((groups: { [key: string]: Event[] }, event) => {
    // Use exact coordinates as the key for precise grouping
    const coordKey = `${event.latitude.toFixed(6)}_${event.longitude.toFixed(6)}`

    if (!groups[coordKey]) {
      groups[coordKey] = []
    }
    groups[coordKey].push(event)
    return groups
  }, {})

  // Merge events at identical coordinates and create display groups
  const mergedEventGroups = Object.entries(groupedEvents).reduce(
    (result: { [key: string]: Event[] }, [coordKey, coordEvents]) => {
      if (coordEvents.length === 1) {
        // Single event - use pixel position for final grouping
        const pos = getPixelPosition(coordEvents[0].latitude, coordEvents[0].longitude)
        const pixelKey = `${Math.round(pos.x / 30) * 30}-${Math.round(pos.y / 30) * 30}`
        if (!result[pixelKey]) result[pixelKey] = []
        result[pixelKey].push(coordEvents[0])
      } else {
        // Multiple events at same coordinates - merge them
        const earthquakes = coordEvents.filter((e) => e.type === "earthquake")
        const fires = coordEvents.filter((e) => e.type === "fire")

        const mergedEvents: Event[] = []

        // Merge earthquakes at same location
        if (earthquakes.length > 0) {
          const maxMagnitudeEvent = earthquakes.reduce((max, current) =>
            (current.magnitude || 0) > (max.magnitude || 0) ? current : max,
          )

          // Create merged earthquake event
          const mergedEarthquake: Event = {
            ...maxMagnitudeEvent,
            id: `merged_earthquake_${coordKey}_${earthquakes.length}`,
            magnitude: Math.max(...earthquakes.map((e) => e.magnitude || 0)),
            // Update severity based on max magnitude
            severity: (() => {
              const maxMag = Math.max(...earthquakes.map((e) => e.magnitude || 0))
              if (maxMag < 4.0) return "low" as const
              if (maxMag < 6.0) return "moderate" as const
              return "high" as const
            })(),
            // Add count info to place name if multiple events
            place:
              earthquakes.length > 1
                ? `${maxMagnitudeEvent.place} (${earthquakes.length} earthquakes)`
                : maxMagnitudeEvent.place,
          }
          mergedEvents.push(mergedEarthquake)
        }

        // Merge fires at same location
        if (fires.length > 0) {
          const maxFrpEvent = fires.reduce((max, current) => ((current.frp || 0) > (max.frp || 0) ? current : max))

          // Create merged fire event
          const mergedFire: Event = {
            ...maxFrpEvent,
            id: `merged_fire_${coordKey}_${fires.length}`,
            frp: Math.max(...fires.map((e) => e.frp || 0)),
            // Update severity based on max FRP
            severity: (() => {
              const maxFrp = Math.max(...fires.map((e) => e.frp || 0))
              if (maxFrp < 5) return "low" as const
              if (maxFrp < 15) return "moderate" as const
              return "high" as const
            })(),
            // Add count info to place name if multiple events
            place: fires.length > 1 ? `${maxFrpEvent.place} (${fires.length} fires)` : maxFrpEvent.place,
          }
          mergedEvents.push(mergedFire)
        }

        // Add merged events to pixel-based grouping
        const pos = getPixelPosition(coordEvents[0].latitude, coordEvents[0].longitude)
        const pixelKey = `${Math.round(pos.x / 30) * 30}-${Math.round(pos.y / 30) * 30}`
        if (!result[pixelKey]) result[pixelKey] = []
        result[pixelKey].push(...mergedEvents)
      }

      return result
    },
    {},
  )

  // Calculate minimum zoom level to fit world in viewport
  const getMinZoomLevel = () => {
    if (!mapRef.current) return 1

    const map = mapRef.current.getMap()
    const mapWidth = map.getCanvas().width
    const mapHeight = map.getCanvas().height

    // Calculate zoom needed to fit world width (360 degrees longitude)
    const zoomForWidth = Math.log2(mapWidth / 256)

    // Calculate zoom needed to fit world height (Web Mercator spans ~85.05° to -85.05°)
    const zoomForHeight = Math.log2(mapHeight / 256)

    // Use the larger zoom to ensure world fills the entire viewport
    const minZoom = Math.max(zoomForWidth, zoomForHeight)

    // Ensure minimum is at least 1 and round up to next integer
    return Math.max(1, Math.ceil(minZoom))
  }

  // Calculate the maximum allowed panning distance in pixels
  const getMaxPanDistance = () => {
    if (!mapRef.current) return { x: 0, y: 0 }

    const map = mapRef.current.getMap()
    const mapWidth = map.getCanvas().width
    const mapHeight = map.getCanvas().height

    // Calculate world size in pixels at current zoom
    const worldSizeInPixels = Math.pow(2, mapState.zoom) * 256

    // Calculate how much of the world is visible in the viewport
    const visibleWorldWidth = Math.min(worldSizeInPixels, mapWidth)
    const visibleWorldHeight = Math.min(worldSizeInPixels, mapHeight)

    // Calculate maximum pan distance (half the difference between world size and viewport)
    const maxPanX = Math.max(0, (worldSizeInPixels - visibleWorldWidth) / 2)
    const maxPanY = Math.max(0, (worldSizeInPixels - visibleWorldHeight) / 2)

    return { x: maxPanX, y: maxPanY }
  }

  // Constrain map center to prevent showing empty space
  const constrainMapBounds = (lat: number, lng: number, zoom: number) => {
    if (!mapRef.current) return { lat, lng }

    const map = mapRef.current.getMap()
    const mapWidth = map.getCanvas().width
    const mapHeight = map.getCanvas().height

    // Calculate world size in pixels at current zoom
    const worldSizeInPixels = Math.pow(2, zoom) * 256

    // If world is smaller than viewport, center it
    if (worldSizeInPixels <= mapWidth && worldSizeInPixels <= mapHeight) {
      return { lat: 0, lng: 0 } // Center the world
    }

    // Calculate the maximum latitude range in Web Mercator projection
    // Web Mercator projection typically goes from about -85.05° to 85.05°
    const maxLat = 85.05

    // Calculate how much of the world is visible in the viewport
    const visibleWorldWidthRatio = Math.min(1, mapWidth / worldSizeInPixels)
    const visibleWorldHeightRatio = Math.min(1, mapHeight / worldSizeInPixels)

    // Calculate maximum longitude offset based on visible portion
    const maxLngOffset = (180 * visibleWorldWidthRatio) / 2

    // Calculate maximum latitude offset based on visible portion and projection limits
    const maxLatOffset = (maxLat * visibleWorldHeightRatio) / 2

    // Constrain longitude (-180 to 180, adjusted for visible portion)
    const maxLng = 180 - maxLngOffset
    const minLng = -180 + maxLngOffset
    const constrainedLng = Math.max(minLng, Math.min(maxLng, lng))

    // Constrain latitude (-85.05 to 85.05, adjusted for visible portion)
    const constrainedMaxLat = maxLat - maxLatOffset
    const constrainedMinLat = -maxLat + maxLatOffset
    const constrainedLat = Math.max(constrainedMinLat, Math.min(constrainedMaxLat, lat))

    return { lat: constrainedLat, lng: constrainedLng }
  }

  // Add this function after the existing helper functions
  const centerOnEvent = (event: Event) => {
    // Apply bounds constraints to ensure the event location is valid
    const constrained = constrainMapBounds(event.latitude, event.longitude, mapState.zoom)

    setMapState((prev) => ({
      ...prev,
      centerLat: constrained.lat,
      centerLng: constrained.lng,
    }))
  }

  const handleZoomOut = () => {
    const minZoom = getMinZoomLevel()
    const newZoom = Math.max(mapState.zoom - 1, minZoom)

    // Constrain center when zooming out
    const constrained = constrainMapBounds(mapState.centerLat, mapState.centerLng, newZoom)

    setMapState((prev) => ({
      zoom: newZoom,
      centerLat: constrained.lat,
      centerLng: constrained.lng,
    }))
  }

  const handleZoomIn = () => {
    setMapState((prev) => {
      const newZoom = Math.min(prev.zoom + 1, 18)
      // Constrain center when zooming in
      const constrained = constrainMapBounds(prev.centerLat, prev.centerLng, newZoom)

      return {
        zoom: newZoom,
        centerLat: constrained.lat,
        centerLng: constrained.lng,
      }
    })
  }

  const handleReset = () => {
    const minZoom = getMinZoomLevel()
    setMapState({
      zoom: Math.max(2, minZoom),
      centerLat: 0,
      centerLng: 0,
    })
  }

  const handleMapClick = (e: React.MouseEvent) => {
    if (!mapRef.current || isDraggingRef.current) return

    const map = mapRef.current.getMap()
    const rect = map.getCanvas().getBoundingClientRect()
    const x = e.clientX - rect.left
    const y = e.clientY - rect.top

    const mapWidth = map.getCanvas().width
    const mapHeight = map.getCanvas().height

    // Convert pixel coordinates back to lat/lng
    const scale = (Math.pow(2, mapState.zoom) * 256) / (2 * Math.PI)
    const centerLatRad = (mapState.centerLat * Math.PI) / 180
    const centerLngRad = (mapState.centerLng * Math.PI) / 180

    const newLngRad = centerLngRad + (x - mapWidth / 2) / scale
    const newLatRad =
      2 *
      (Math.atan(Math.exp((mapHeight / 2 - y) / scale + Math.log(Math.tan(Math.PI / 4 + centerLatRad / 2)))) -
        Math.PI / 4)

    const newLat = (newLatRad * 180) / Math.PI
    const newLng = (newLngRad * 180) / Math.PI

    // Apply bounds constraints
    const constrained = constrainMapBounds(newLat, newLng, mapState.zoom)

    setMapState((prev) => ({
      ...prev,
      centerLat: constrained.lat,
      centerLng: constrained.lng,
    }))
  }

  // Mouse drag handlers for smoother panning
  const handleMouseDown = (e: React.MouseEvent) => {
    if (!mapRef.current) return

    isDraggingRef.current = true
    lastMousePosRef.current = { x: e.clientX, y: e.clientY }

    // Prevent text selection during drag
    e.preventDefault()
  }

  const handleMouseMove = (e: React.MouseEvent) => {
    if (!isDraggingRef.current || !mapRef.current) return

    const dx = e.clientX - lastMousePosRef.current.x
    const dy = e.clientY - lastMousePosRef.current.y

    // Convert pixel movement to lat/lng movement
    const scale = (Math.pow(2, mapState.zoom) * 256) / (2 * Math.PI)
    const dLng = (-dx / scale) * (180 / Math.PI)

    // Latitude movement is more complex due to Mercator projection
    const centerLatRad = (mapState.centerLat * Math.PI) / 180
    const factor = Math.cos(centerLatRad)
    const dLat = ((dy / scale) * (180 / Math.PI)) / factor

    // Apply the movement with constraints
    const newLat = mapState.centerLat + dLat
    const newLng = mapState.centerLng + dLng

    // Apply bounds constraints
    const constrained = constrainMapBounds(newLat, newLng, mapState.zoom)

    setMapState((prev) => ({
      ...prev,
      centerLat: constrained.lat,
      centerLng: constrained.lng,
    }))

    // Update last position
    lastMousePosRef.current = { x: e.clientX, y: e.clientY }
  }

  const handleMouseUp = () => {
    isDraggingRef.current = false
  }

  // Also handle mouse leaving the map
  const handleMouseLeave = () => {
    isDraggingRef.current = false
  }

  // Get tile URL based on map style
  const getTileUrl = (x: number, y: number, z: number) => {
    // Handle tile coordinates that are out of bounds
    const maxTile = Math.pow(2, z) - 1

    // If tile is out of bounds, return null
    if (x < 0 || y < 0 || x > maxTile || y > maxTile) {
      return null
    }

    switch (mapStyle) {
      case "street":
        return `https://tile.openstreetmap.org/${z}/${x}/${y}.png`
      case "satellite":
        // Using Esri World Imagery (free satellite tiles)
        return `https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/${z}/${y}/${x}`
      case "terrain":
        // Using OpenTopoMap for terrain
        return `https://tile.opentopomap.org/${z}/${x}/${y}.png`
      case "dark":
        // Using CartoDB Dark Matter
        return `https://cartodb-basemaps-a.global.ssl.fastly.net/dark_all/${z}/${x}/${y}.png`
      default:
        return `https://tile.openstreetmap.org/${z}/${x}/${y}.png`
    }
  }

  const getTilesForView = () => {
    const tileSize = 256
    const scale = Math.pow(2, mapState.zoom)
    const centerX = ((mapState.centerLng + 180) / 360) * scale
    const centerY =
      ((1 -
        Math.log(Math.tan((mapState.centerLat * Math.PI) / 180) + 1 / Math.cos((mapState.centerLat * Math.PI) / 180)) /
        Math.PI) /
        2) *
      scale

    const mapWidth = mapRef.current?.getMap()?.getCanvas().width || 800
    const mapHeight = mapRef.current?.getMap()?.getCanvas().height || 400

    const tilesX = Math.ceil(mapWidth / tileSize) + 2
    const tilesY = Math.ceil(mapHeight / tileSize) + 2

    const startX = Math.floor(centerX - tilesX / 2)
    const startY = Math.floor(centerY - tilesY / 2)

    const tiles = []
    for (let x = startX; x < startX + tilesX; x++) {
      for (let y = startY; y < startY + tilesY; y++) {
        // Calculate wrapped tile coordinates to handle world wrapping
        const wrappedX = ((x % scale) + scale) % scale
        const wrappedY = ((y % scale) + scale) % scale

        if (wrappedY >= 0 && wrappedY < scale) {
          // Y needs to be in bounds
          const pixelX = (x - centerX) * tileSize + mapWidth / 2
          const pixelY = (y - centerY) * tileSize + mapHeight / 2

          const tileUrl = getTileUrl(wrappedX, wrappedY, mapState.zoom)

          if (tileUrl) {
            tiles.push({
              url: tileUrl,
              x: pixelX,
              y: pixelY,
              key: `${x}-${y}-${mapState.zoom}-${mapStyle}`,
            })
          }
        }
      }
    }
    return tiles
  }

  const getMapStyleInfo = (style: MapStyle) => {
    switch (style) {
      case "street":
        return { name: "Street Map", icon: Map, description: "Standard street map with roads and labels" }
      case "satellite":
        return { name: "Satellite", icon: Satellite, description: "High-resolution satellite imagery" }
      case "terrain":
        return { name: "Terrain", icon: Map, description: "Topographic map with elevation data" }
      case "dark":
        return { name: "Dark Mode", icon: Map, description: "Dark themed map for low-light viewing" }
      default:
        return { name: "Street Map", icon: Map, description: "Standard street map" }
    }
  }

  const getMarkerColor = (event: Event) => {
    const severityColors = {
      high: "text-red-600",
      moderate: "text-yellow-500",
      low: "text-green-500",
      default: "text-gray-500"
    }

    return severityColors[event.severity as keyof typeof severityColors] || severityColors.default
  }

  const getMarkerIcon = (event: Event) => {
    if (event.type === "earthquake") {
      return (
        <svg
          viewBox="0 0 24 24"
          fill="currentColor"
          className="w-5 h-5 drop-shadow-lg"
        >
          <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-9-4.03-9-9s4.03-9 9-9 9 4.03 9 9-4.03 9-9 9zm-1-13h2v6h-2zm0 8h2v2h-2z" />
        </svg>
      )
    } else {
      return (
        <svg
          viewBox="0 0 24 24"
          fill="currentColor"
          className="w-5 h-5 drop-shadow-lg"
        >
          <path d="M12 23c-4.97 0-9-4.03-9-9s4.03-9 9-9 9 4.03 9 9-4.03 9-9 9zm0-16c-3.87 0-7 3.13-7 7s3.13 7 7 7 7-3.13 7-7-3.13-7-7-7zm0 11c-2.21 0-4-1.79-4-4s1.79-4 4-4 4 1.79 4 4-1.79 4-4 4z" />
        </svg>
      )
    }
  }

  useEffect(() => {
    if (mapRef.current && events.length > 0) {
      const map = mapRef.current.getMap()
      const bounds = new Mapbox.LngLatBounds()
      events.forEach(event => {
        bounds.extend([event.longitude, event.latitude])
      })
      map.fitBounds(bounds, { padding: 50, duration: 1000 })
    }
  }, [events])

  // Add this useEffect after the existing one
  useEffect(() => {
    if (selectedEventId) {
      // First try to find the exact event
      let selectedEvent = events.find((e) => e.id === selectedEventId)

      // If not found, it might be a merged event, so search in the original events
      if (!selectedEvent) {
        // Look for the original event that might have been merged
        selectedEvent = events.find(
          (e) =>
            selectedEventId.includes(e.type) &&
            selectedEventId.includes(e.latitude.toFixed(6)) &&
            selectedEventId.includes(e.longitude.toFixed(6)),
        )
      }

      if (selectedEvent) {
        console.log("Centering map on event:", {
          id: selectedEvent.id,
          type: selectedEvent.type,
          place: selectedEvent.place,
        })
        centerOnEvent(selectedEvent)
      } else {
        console.log("Event not found for ID:", selectedEventId)
      }
    }
  }, [selectedEventId, events])

  // Add effect to handle zooming to selected event
  useEffect(() => {
    if (selectedEventId && mapRef.current) {
      const selectedEvent = events.find(e => e.id === selectedEventId)
      if (selectedEvent) {
        mapRef.current.flyTo({
          center: [selectedEvent.longitude, selectedEvent.latitude],
          zoom: 8,
          duration: 1000
        })
      }
    }
  }, [selectedEventId, events])

  // Add error handling for map load
  const handleMapLoad = () => {
    console.log('Map loaded successfully')
    setMapLoaded(true)
    setMapError(null)
  }

  const handleMapError = (e: { error: Error }) => {
    console.error('Map error:', e.error)
    setMapError(e.error.message)
  }

  const styleInfo = getMapStyleInfo(mapStyle)

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <MapPin className="h-5 w-5" />
            <span>Global Event Map</span>
            <Badge variant="outline" className="ml-2">
              {events.length} events
            </Badge>
          </div>
          <div className="flex items-center space-x-2">
            {/* Map Style Selector */}

          </div>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="relative h-[600px] w-full rounded-lg overflow-hidden shadow-lg border border-gray-100">
          <div className="absolute top-4 right-4 z-10 bg-white rounded-lg shadow-md p-2 text-sm text-gray-600">
            {events.length} events shown
          </div>
          {mapError && (
            <div className="absolute inset-0 flex items-center justify-center bg-red-50">
              <div className="text-red-600 p-4 rounded-lg bg-white shadow-lg">
                Error loading map: {mapError}
              </div>
            </div>
          )}
          <ReactMap
            ref={mapRef}
            mapboxAccessToken={process.env.NEXT_PUBLIC_MAPBOX_TOKEN}
            mapStyle="mapbox://styles/mapbox/light-v11"
            onLoad={handleMapLoad}
            onError={handleMapError}
            style={{ width: '100%', height: '100%' }}
            initialViewState={{
              longitude: 0,
              latitude: 20,
              zoom: 2
            }}
          >
            {mapLoaded && (
              <>
                {events.map((event) => (
                  <Marker
                    key={event.id}
                    longitude={event.longitude}
                    latitude={event.latitude}
                    onClick={(e) => {
                      e.originalEvent.stopPropagation()
                      onEventSelect?.(event.id)
                    }}
                  >
                    <div
                      className={`transform transition-all duration-200 ${selectedEventId === event.id
                        ? "scale-110 ring-2 ring-white ring-offset-2 ring-offset-blue-500"
                        : "hover:scale-105 hover:ring-1 hover:ring-white"
                        } ${getMarkerColor(event)}`}
                      onMouseEnter={() => setHoveredEventId(event.id)}
                      onMouseLeave={() => setHoveredEventId(null)}
                    >
                      {getMarkerIcon(event)}
                    </div>
                  </Marker>
                ))}
                {hoveredEventId && (
                  <Popup
                    longitude={events.find(e => e.id === hoveredEventId)?.longitude || 0}
                    latitude={events.find(e => e.id === hoveredEventId)?.latitude || 0}
                    anchor="bottom"
                    closeButton={false}
                    closeOnClick={false}
                    onClose={() => setHoveredEventId(null)}
                    offset={20}
                  >
                    {(() => {
                      const event = events.find(e => e.id === hoveredEventId)
                      if (!event) return null
                      return (
                        <div className="p-2 min-w-[200px]">
                          <div className="flex items-center gap-2 mb-2">
                            {event.type === "earthquake" ? (
                              <Zap className="h-4 w-4 text-orange-500" />
                            ) : (
                              <Flame className="h-4 w-4 text-red-500" />
                            )}
                            <span className="font-medium">
                              {event.type === "earthquake" ? "Earthquake" : "Fire"}
                            </span>
                            <Badge
                              variant="outline"
                              className={`text-xs ${getSeverityColor(event.severity)}`}
                            >
                              {event.severity.charAt(0).toUpperCase() + event.severity.slice(1)}
                            </Badge>
                          </div>
                          <div className="space-y-1 text-sm">
                            <p className="flex items-center gap-1 text-gray-600">
                              <MapPin className="h-3 w-3" />
                              {event.place}
                            </p>
                            <p className="flex items-center gap-1 text-gray-500">
                              <Clock className="h-3 w-3" />
                              {formatTime(event.time)}
                            </p>
                            {event.type === "earthquake" && (
                              <p className="text-sm font-medium text-gray-900">
                                Magnitude {event.magnitude}
                              </p>
                            )}
                          </div>
                        </div>
                      )
                    })()}
                  </Popup>
                )}
                <NavigationControl
                  position="bottom-right"
                  showCompass={false}
                  style={{ bottom: '1rem', right: '1rem' }}
                />
              </>
            )}
          </ReactMap>
          {!mapLoaded && !mapError && (
            <div className="absolute inset-0 flex items-center justify-center bg-gray-50">
              <div className="flex flex-col items-center gap-2">
                <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin" />
                <span className="text-sm text-gray-600">Loading map...</span>
              </div>
            </div>
          )}
        </div>


        
      </CardContent>
    </Card>
  )
}
