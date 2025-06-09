"use client"

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { AlertTriangle, Filter, ChevronDown, ChevronUp } from "lucide-react"
import { TabsContent } from "@/components/ui/tabs"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { useState, ChangeEvent } from "react"
import { Badge } from "@/components/ui/badge"

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

export function AnomaliesTab() {
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
        <TabsContent value="anomalies">
            <Card>
                <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                        <div>
                            <CardTitle className="text-2xl">Anomalous Disasters</CardTitle>
                            <CardDescription className="mt-1.5">
                                Events that significantly exceed normal thresholds for magnitude, deaths, or affected population
                            </CardDescription>
                        </div>
                        <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setShowFilters(!showFilters)}
                            className="flex items-center gap-2"
                        >
                            <Filter className="h-4 w-4" />
                            {showFilters ? (
                                <>
                                    Hide Filters
                                    <ChevronUp className="h-4 w-4" />
                                </>
                            ) : (
                                <>
                                    Show Filters
                                    <ChevronDown className="h-4 w-4" />
                                </>
                            )}
                        </Button>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    {/* Filters */}
                    {showFilters && (
                        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 p-4 bg-gray-50 rounded-lg border border-gray-100">
                            <div className="space-y-2">
                                <label className="text-sm font-medium text-gray-700">Disaster Type</label>
                                <Select value={selectedDisasterType} onValueChange={setSelectedDisasterType}>
                                    <SelectTrigger>
                                        <SelectValue placeholder="Select type" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="all">All Types</SelectItem>
                                        <SelectItem value="earthquake">Earthquake</SelectItem>
                                        <SelectItem value="flood">Flood</SelectItem>
                                        <SelectItem value="storm">Storm</SelectItem>
                                        <SelectItem value="wildfire">Wildfire</SelectItem>
                                        <SelectItem value="volcanic activity">Volcanic Activity</SelectItem>
                                        <SelectItem value="tsunami">Tsunami</SelectItem>
                                        <SelectItem value="drought">Drought</SelectItem>
                                        <SelectItem value="landslide">Landslide</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>

                            <div className="space-y-2">
                                <label className="text-sm font-medium text-gray-700">Anomaly Type</label>
                                <Select value={selectedAnomalyType} onValueChange={setSelectedAnomalyType}>
                                    <SelectTrigger>
                                        <SelectValue placeholder="Select anomaly" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="all">All Anomalies</SelectItem>
                                        <SelectItem value="magnitude">
                                            <div className="flex items-center gap-2">
                                                <span className="w-2 h-2 rounded-full bg-blue-500" />
                                                Magnitude
                                            </div>
                                        </SelectItem>
                                        <SelectItem value="deaths">
                                            <div className="flex items-center gap-2">
                                                <span className="w-2 h-2 rounded-full bg-red-500" />
                                                Deaths
                                            </div>
                                        </SelectItem>
                                        <SelectItem value="affected">
                                            <div className="flex items-center gap-2">
                                                <span className="w-2 h-2 rounded-full bg-orange-500" />
                                                Affected
                                            </div>
                                        </SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>

                            <div className="space-y-2">
                                <label className="text-sm font-medium text-gray-700">Year Range</label>
                                <div className="flex gap-2">
                                    <Input
                                        type="number"
                                        placeholder="Start Year"
                                        value={yearRange.start}
                                        onChange={handleYearChange('start')}
                                        className="w-full"
                                    />
                                    <Input
                                        type="number"
                                        placeholder="End Year"
                                        value={yearRange.end}
                                        onChange={handleYearChange('end')}
                                        className="w-full"
                                    />
                                </div>
                            </div>

                            <div className="flex items-end">
                                <Button className="w-full">Apply Filters</Button>
                            </div>
                        </div>
                    )}

                    {/* Data Table */}
                    <div className="rounded-lg border border-gray-200 bg-white">
                        <div className="p-4 border-b border-gray-200 bg-gray-50">
                            <div className="flex items-center justify-between">
                                <h3 className="font-medium">Anomaly Results</h3>
                                <div className="flex items-center gap-2">
                                    <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200">
                                        Magnitude
                                    </Badge>
                                    <Badge variant="outline" className="bg-red-50 text-red-700 border-red-200">
                                        Deaths
                                    </Badge>
                                    <Badge variant="outline" className="bg-orange-50 text-orange-700 border-orange-200">
                                        Affected
                                    </Badge>
                                </div>
                            </div>
                        </div>
                        <Table>
                            <TableHeader>
                                <TableRow className="bg-gray-50 hover:bg-gray-50">
                                    <TableHead>Disaster Type</TableHead>
                                    <TableHead>Country</TableHead>
                                    <TableHead>Event Name</TableHead>
                                    <TableHead>Date</TableHead>
                                    <TableHead>Last Updated</TableHead>
                                    <TableHead>Magnitude</TableHead>
                                    <TableHead>Deaths</TableHead>
                                    <TableHead>Affected</TableHead>
                                    <TableHead>Anomaly Type</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {filteredAnomalies.length === 0 ? (
                                    <TableRow>
                                        <TableCell colSpan={9} className="text-center text-gray-500 py-8">
                                            <div className="flex flex-col items-center gap-2">
                                                <AlertTriangle className="h-8 w-8 text-gray-400" />
                                                <p>No anomalies found</p>
                                                <p className="text-sm">Try adjusting the filters to see more results</p>
                                            </div>
                                        </TableCell>
                                    </TableRow>
                                ) : (
                                    paginatedAnomalies.map((anomaly) => (
                                        <TableRow key={anomaly.id} className="hover:bg-gray-50">
                                            <TableCell className="font-medium capitalize">
                                                {anomaly.disasterType}
                                            </TableCell>
                                            <TableCell>{anomaly.country}</TableCell>
                                            <TableCell>
                                                <a
                                                    href={anomaly.source}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-blue-600 hover:underline"
                                                >
                                                    {anomaly.eventName}
                                                </a>
                                            </TableCell>
                                            <TableCell>
                                                {new Date(anomaly.date).toLocaleDateString()}
                                            </TableCell>
                                            <TableCell>
                                                {new Date(anomaly.lastUpdated).toLocaleDateString()}
                                            </TableCell>
                                            <TableCell className="text-right font-medium">
                                                {anomaly.magnitude?.toFixed(2) ?? "N/A"}
                                            </TableCell>
                                            <TableCell className="text-right font-medium">
                                                {anomaly.deaths?.toLocaleString() ?? "N/A"}
                                            </TableCell>
                                            <TableCell className="text-right font-medium">
                                                {anomaly.affected?.toLocaleString() ?? "N/A"}
                                            </TableCell>
                                            <TableCell>
                                                <Badge
                                                    variant="outline"
                                                    className={`${getAnomalyTypeColor(anomaly.anomalyType)}`}
                                                >
                                                    {anomaly.anomalyType}
                                                </Badge>
                                            </TableCell>
                                        </TableRow>
                                    ))
                                )}
                            </TableBody>
                        </Table>
                    </div>

                    {/* Pagination */}
                    {filteredAnomalies.length > 0 && (
                        <div className="flex items-center justify-between pt-4">
                            <div className="text-sm text-gray-500">
                                Showing {((currentPage - 1) * itemsPerPage) + 1} to {Math.min(currentPage * itemsPerPage, filteredAnomalies.length)} of {filteredAnomalies.length} results
                            </div>
                            <div className="flex gap-2">
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                                    disabled={currentPage === 1}
                                >
                                    Previous
                                </Button>
                                <Button
                                    variant="outline"
                                    size="sm"
                                    onClick={() => setCurrentPage(prev => prev + 1)}
                                    disabled={currentPage * itemsPerPage >= filteredAnomalies.length}
                                >
                                    Next
                                </Button>
                            </div>
                        </div>
                    )}
                </CardContent>
            </Card>
        </TabsContent>
    )
} 