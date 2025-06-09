"use client"

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Clock, AlertTriangle, Calendar, MapPin, BarChart2, Globe, Filter, ChevronDown, ChevronUp, Loader2 } from "lucide-react"
import Link from "next/link"
import { Header } from "../components/dashboard/Header"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { useState, ChangeEvent } from "react"
import { Badge } from "@/components/ui/badge"
import { CorrelationsTab } from "../components/dashboard/CorrelationsTab"
import { AnomaliesTab } from "@/app/components/dashboard/AnomaliesTab"
import { HotspotsTab } from "@/app/components/dashboard/HotspotsTab"

// Types for our mock data
interface AnomalyEvent {
    id: string
    eventName: string
    disasterType: string
    country: string
    date: string
    lastUpdated: string
    magnitude: number | null
    deaths: number | null
    affected: number | null
    anomalyType: "magnitude" | "deaths" | "affected"
    source: string
    coordinates: [number, number]
}

// Mock data for testing
const mockAnomalies: AnomalyEvent[] = [
    {
        id: "1",
        eventName: "Great Sumatra Earthquake",
        disasterType: "Earthquake",
        country: "Indonesia",
        date: "2024-01-15",
        lastUpdated: "2024-01-16",
        magnitude: 8.7,
        deaths: 1200,
        affected: 50000,
        anomalyType: "magnitude",
        source: "https://example.com/sumatra-quake",
        coordinates: [3.3167, 95.8500]
    },
    {
        id: "2",
        eventName: "Philippines Super Typhoon",
        disasterType: "Storm",
        country: "Philippines",
        date: "2024-01-20",
        lastUpdated: "2024-01-21",
        magnitude: null,
        deaths: 2500,
        affected: 150000,
        anomalyType: "deaths",
        source: "https://example.com/philippines-typhoon",
        coordinates: [14.5995, 120.9842]
    },
    {
        id: "3",
        eventName: "Australian Bushfire Crisis",
        disasterType: "Wildfire",
        country: "Australia",
        date: "2024-01-10",
        lastUpdated: "2024-01-25",
        magnitude: null,
        deaths: 150,
        affected: 200000,
        anomalyType: "affected",
        source: "https://example.com/australia-fires",
        coordinates: [-33.8688, 151.2093]
    },
    {
        id: "4",
        eventName: "Himalayan Avalanche",
        disasterType: "Landslide",
        country: "Nepal",
        date: "2024-01-05",
        lastUpdated: "2024-01-06",
        magnitude: null,
        deaths: 800,
        affected: 10000,
        anomalyType: "deaths",
        source: "https://example.com/himalayan-avalanche",
        coordinates: [27.7172, 85.3240]
    },
    {
        id: "5",
        eventName: "Amazon Flood Crisis",
        disasterType: "Flood",
        country: "Brazil",
        date: "2024-01-18",
        lastUpdated: "2024-01-22",
        magnitude: null,
        deaths: 300,
        affected: 300000,
        anomalyType: "affected",
        source: "https://example.com/amazon-floods",
        coordinates: [-3.1190, -60.0217]
    },
    {
        id: "6",
        eventName: "Japanese Tsunami",
        disasterType: "Tsunami",
        country: "Japan",
        date: "2024-01-12",
        lastUpdated: "2024-01-13",
        magnitude: 7.8,
        deaths: 500,
        affected: 75000,
        anomalyType: "magnitude",
        source: "https://example.com/japan-tsunami",
        coordinates: [35.6762, 139.6503]
    },
    {
        id: "7",
        eventName: "California Mega Drought",
        disasterType: "Drought",
        country: "United States",
        date: "2024-01-01",
        lastUpdated: "2024-01-30",
        magnitude: null,
        deaths: 50,
        affected: 1000000,
        anomalyType: "affected",
        source: "https://example.com/california-drought",
        coordinates: [36.7783, -119.4179]
    },
    {
        id: "8",
        eventName: "Hawaiian Volcanic Eruption",
        disasterType: "Volcanic Activity",
        country: "United States",
        date: "2024-01-25",
        lastUpdated: "2024-01-26",
        magnitude: 6.2,
        deaths: 0,
        affected: 25000,
        anomalyType: "magnitude",
        source: "https://example.com/hawaii-volcano",
        coordinates: [19.8968, -155.5828]
    }
]

export default function BatchDashboard() {
    // State for anomalies tab
    const [selectedDisasterType, setSelectedDisasterType] = useState<string>("all")
    const [yearRange, setYearRange] = useState<{ start: string; end: string }>({ start: "2000", end: "2024" })
    const [selectedAnomalyType, setSelectedAnomalyType] = useState<string>("all")
    const [currentPage, setCurrentPage] = useState(1)
    const [showFilters, setShowFilters] = useState(true)
    const itemsPerPage = 10

    const handleYearChange = (field: 'start' | 'end') => (e: ChangeEvent<HTMLInputElement>) => {
        setYearRange(prev => ({ ...prev, [field]: e.target.value }))
    }

    const getAnomalyTypeColor = (type: "magnitude" | "deaths" | "affected") => {
        switch (type) {
            case "magnitude":
                return "bg-blue-100 text-blue-700 border-blue-200"
            case "deaths":
                return "bg-red-100 text-red-700 border-red-200"
            case "affected":
                return "bg-orange-100 text-orange-700 border-orange-200"
        }
    }

    const filteredAnomalies = mockAnomalies.filter(anomaly => {
        const matchesType = selectedDisasterType === "all" ||
            anomaly.disasterType.toLowerCase() === selectedDisasterType.toLowerCase()
        const matchesAnomalyType = selectedAnomalyType === "all" ||
            anomaly.anomalyType === selectedAnomalyType
        const eventYear = new Date(anomaly.date).getFullYear()
        const matchesYear = eventYear >= parseInt(yearRange.start) &&
            eventYear <= parseInt(yearRange.end)
        return matchesType && matchesAnomalyType && matchesYear
    })

    const paginatedAnomalies = filteredAnomalies.slice(
        (currentPage - 1) * itemsPerPage,
        currentPage * itemsPerPage
    )

    return (
        <div className="min-h-screen bg-gray-50">
            <div className="container mx-auto px-4 py-6 space-y-6">
                <Header
                    lastUpdated={new Date()}
                    useRealData={true}
                    isRealTime={false}
                    isLoading={false}
                    onToggleDataMode={() => { }}
                    onToggleRealTime={() => { }}
                    onRefresh={() => { }}
                    liveLink={
                        <Link
                            href="/"
                            className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-50 hover:bg-gray-100 text-gray-500 rounded-full transition-colors cursor-pointer"
                        >
                            <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
                            <span className="text-sm font-medium">Live</span>
                        </Link>
                    }
                    historicalLink={
                        <div className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-50 text-blue-700 rounded-full">
                            <Clock className="h-4 w-4" />
                            <span className="text-sm font-medium">Historical</span>
                        </div>
                    }
                />

                <Tabs defaultValue="anomalies" className="space-y-4">
                    <TabsList className="grid w-full grid-cols-5">
                        <TabsTrigger value="anomalies" className="flex items-center gap-2">
                            <AlertTriangle className="h-4 w-4" />
                            Anomalies
                        </TabsTrigger>
                        <TabsTrigger value="correlations" className="flex items-center gap-2">
                            <Calendar className="h-4 w-4" />
                            Correlations
                        </TabsTrigger>
                        <TabsTrigger value="hotspots" className="flex items-center gap-2">
                            <MapPin className="h-4 w-4" />
                            Hotspots
                        </TabsTrigger>
                        <TabsTrigger value="vulnerability" className="flex items-center gap-2">
                            <Globe className="h-4 w-4" />
                            Vulnerability
                        </TabsTrigger>
                        <TabsTrigger value="summaries" className="flex items-center gap-2">
                            <BarChart2 className="h-4 w-4" />
                            Summaries
                        </TabsTrigger>
                    </TabsList>

                    <TabsContent value="anomalies">
                        <AnomaliesTab />
                    </TabsContent>

                    <TabsContent value="correlations">
                        <Card>
                            <CardHeader>
                                <CardTitle>Temporally Correlated Disasters</CardTitle>
                                <CardDescription>
                                    Analysis of disasters that occurred within 30 days of each other
                                </CardDescription>
                            </CardHeader>
                            <CardContent>
                                <p className="text-sm text-muted-foreground">Coming soon...</p>
                            </CardContent>
                        </Card>
                    </TabsContent>

                    <TabsContent value="hotspots">
                        <HotspotsTab />
                    </TabsContent>

                    <TabsContent value="vulnerability">
                        <Card>
                            <CardHeader>
                                <CardTitle>Country Vulnerability Index</CardTitle>
                                <CardDescription>
                                    Analysis of country-level vulnerability to different types of disasters
                                </CardDescription>
                            </CardHeader>
                            <CardContent>
                                <p className="text-sm text-muted-foreground">Coming soon...</p>
                            </CardContent>
                        </Card>
                    </TabsContent>

                    <TabsContent value="summaries">
                        <Card>
                            <CardHeader>
                                <CardTitle>Disaster Summaries</CardTitle>
                                <CardDescription>
                                    Detailed summaries for each disaster type
                                </CardDescription>
                            </CardHeader>
                            <CardContent>
                                <p className="text-sm text-muted-foreground">Coming soon...</p>
                            </CardContent>
                        </Card>
                    </TabsContent>
                </Tabs>
            </div>
        </div>
    )
} 