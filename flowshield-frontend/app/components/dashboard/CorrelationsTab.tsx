"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { TabsContent } from "@/components/ui/tabs"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { AlertTriangle, Calendar, Clock, MapPin, ChevronUp, ChevronDown } from "lucide-react"

// Types for our mock data
interface CorrelatedEvent {
    id: string
    country: string
    disasterTypeA: string
    startDateA: string
    eventNameA: string
    disasterTypeB: string
    startDateB: string
    eventNameB: string
    daysBetween: number
}

// Mock data for testing
const mockCorrelations: CorrelatedEvent[] = [
    {
        id: "1",
        country: "Indonesia",
        disasterTypeA: "Earthquake",
        startDateA: "2024-01-15",
        eventNameA: "Sumatra Earthquake",
        disasterTypeB: "Tsunami",
        startDateB: "2024-01-16",
        eventNameB: "Indian Ocean Tsunami",
        daysBetween: 1
    },
    {
        id: "2",
        country: "Philippines",
        disasterTypeA: "Typhoon",
        startDateA: "2024-01-20",
        eventNameA: "Super Typhoon Haiyan",
        disasterTypeB: "Flood",
        startDateB: "2024-01-22",
        eventNameB: "Manila Flood Crisis",
        daysBetween: 2
    },
    {
        id: "3",
        country: "Australia",
        disasterTypeA: "Wildfire",
        startDateA: "2024-01-10",
        eventNameA: "Bushfire Crisis",
        disasterTypeB: "Drought",
        startDateB: "2024-01-15",
        eventNameB: "Eastern Drought",
        daysBetween: 5
    },
    {
        id: "4",
        country: "Japan",
        disasterTypeA: "Earthquake",
        startDateA: "2024-01-05",
        eventNameA: "Tohoku Earthquake",
        disasterTypeB: "Volcanic Activity",
        startDateB: "2024-01-08",
        eventNameB: "Mount Fuji Activity",
        daysBetween: 3
    },
    {
        id: "5",
        country: "United States",
        disasterTypeA: "Hurricane",
        startDateA: "2024-01-18",
        eventNameA: "Hurricane Katrina",
        disasterTypeB: "Flood",
        startDateB: "2024-01-20",
        eventNameB: "New Orleans Flood",
        daysBetween: 2
    },
    {
        id: "6",
        country: "Italy",
        disasterTypeA: "Earthquake",
        startDateA: "2024-01-12",
        eventNameA: "Central Italy Earthquake",
        disasterTypeB: "Landslide",
        startDateB: "2024-01-14",
        eventNameB: "Abruzzo Landslide",
        daysBetween: 2
    },
    {
        id: "7",
        country: "Brazil",
        disasterTypeA: "Flood",
        startDateA: "2024-01-01",
        eventNameA: "Amazon Flood",
        disasterTypeB: "Landslide",
        startDateB: "2024-01-05",
        eventNameB: "Rio de Janeiro Landslide",
        daysBetween: 4
    },
    {
        id: "8",
        country: "India",
        disasterTypeA: "Cyclone",
        startDateA: "2024-01-25",
        eventNameA: "Cyclone Amphan",
        disasterTypeB: "Flood",
        startDateB: "2024-01-27",
        eventNameB: "Kolkata Flood",
        daysBetween: 2
    }
]

export function CorrelationsTab() {
    const [selectedCountry, setSelectedCountry] = useState<string>("all")
    const [selectedDisasterType, setSelectedDisasterType] = useState<string>("all")
    const [selectedDaysBetween, setSelectedDaysBetween] = useState<string>("all")
    const [searchQuery, setSearchQuery] = useState("")
    const [currentPage, setCurrentPage] = useState(1)
    const itemsPerPage = 8
    const [sortBy, setSortBy] = useState<keyof CorrelatedEvent | null>(null)
    const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc")

    // Get unique countries and disaster types for filters
    const countries = Array.from(new Set(mockCorrelations.map(c => c.country)))
    const disasterTypes = Array.from(new Set([
        ...mockCorrelations.map(c => c.disasterTypeA),
        ...mockCorrelations.map(c => c.disasterTypeB)
    ]))

    const getDisasterTypeColor = (type: string) => {
        switch (type.toLowerCase()) {
            case "earthquake":
                return "bg-blue-100 text-blue-700 border-blue-200"
            case "tsunami":
                return "bg-cyan-100 text-cyan-700 border-cyan-200"
            case "typhoon":
            case "hurricane":
            case "cyclone":
                return "bg-purple-100 text-purple-700 border-purple-200"
            case "flood":
                return "bg-indigo-100 text-indigo-700 border-indigo-200"
            case "wildfire":
                return "bg-orange-100 text-orange-700 border-orange-200"
            case "drought":
                return "bg-yellow-100 text-yellow-700 border-yellow-200"
            case "volcanic activity":
                return "bg-red-100 text-red-700 border-red-200"
            case "landslide":
                return "bg-brown-100 text-brown-700 border-brown-200"
            default:
                return "bg-gray-100 text-gray-700 border-gray-200"
        }
    }

    let filteredCorrelations = mockCorrelations.filter(correlation => {
        const matchesCountry = selectedCountry === "all" || correlation.country === selectedCountry
        const matchesDisasterType = selectedDisasterType === "all" ||
            correlation.disasterTypeA === selectedDisasterType ||
            correlation.disasterTypeB === selectedDisasterType
        const matchesDaysBetween = selectedDaysBetween === "all" ||
            correlation.daysBetween === parseInt(selectedDaysBetween)
        const matchesSearch = searchQuery === "" ||
            correlation.country.toLowerCase().includes(searchQuery.toLowerCase()) ||
            correlation.eventNameA.toLowerCase().includes(searchQuery.toLowerCase()) ||
            correlation.eventNameB.toLowerCase().includes(searchQuery.toLowerCase())

        return matchesCountry && matchesDisasterType && matchesDaysBetween && matchesSearch
    })

    // Sorting
    if (sortBy) {
        filteredCorrelations = [...filteredCorrelations].sort((a, b) => {
            let aValue = a[sortBy]
            let bValue = b[sortBy]
            if (sortBy === "startDateA" || sortBy === "startDateB") {
                aValue = new Date(aValue as string).getTime()
                bValue = new Date(bValue as string).getTime()
            }
            if (aValue < bValue) return sortDirection === "asc" ? -1 : 1
            if (aValue > bValue) return sortDirection === "asc" ? 1 : -1
            return 0
        })
    }

    // Pagination
    const totalResults = filteredCorrelations.length
    const paginatedCorrelations = filteredCorrelations.slice(
        (currentPage - 1) * itemsPerPage,
        currentPage * itemsPerPage
    )

    const handleSort = (column: keyof CorrelatedEvent) => {
        if (sortBy === column) {
            setSortDirection(sortDirection === "asc" ? "desc" : "asc")
        } else {
            setSortBy(column)
            setSortDirection("asc")
        }
    }

    return (
        <TabsContent value="correlations">
            <Card>
                <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                        <div>
                            <CardTitle className="text-2xl">Temporally Correlated Disasters</CardTitle>
                            <CardDescription className="mt-1.5">
                                Pairs of different disaster types that occurred within 7 days of each other in the same country
                            </CardDescription>
                        </div>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    {/* Filters */}
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4 p-4 bg-gray-50 rounded-lg border border-gray-100">
                        <div className="space-y-2">
                            <label className="text-sm font-medium text-gray-700">Country</label>
                            <Select value={selectedCountry} onValueChange={setSelectedCountry}>
                                <SelectTrigger>
                                    <SelectValue placeholder="Select country" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="all">All Countries</SelectItem>
                                    {countries.map(country => (
                                        <SelectItem key={country} value={country}>{country}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        <div className="space-y-2">
                            <label className="text-sm font-medium text-gray-700">Disaster Type</label>
                            <Select value={selectedDisasterType} onValueChange={setSelectedDisasterType}>
                                <SelectTrigger>
                                    <SelectValue placeholder="Select type" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="all">All Types</SelectItem>
                                    {disasterTypes.map(type => (
                                        <SelectItem key={type} value={type}>{type}</SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        <div className="space-y-2">
                            <label className="text-sm font-medium text-gray-700">Days Between</label>
                            <Select value={selectedDaysBetween} onValueChange={setSelectedDaysBetween}>
                                <SelectTrigger>
                                    <SelectValue placeholder="Select days" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="all">All Intervals</SelectItem>
                                    {[0, 1, 2, 3, 4, 5, 6, 7].map(days => (
                                        <SelectItem key={days} value={days.toString()}>
                                            {days === 0 ? "Same day" : `${days} day${days === 1 ? "" : "s"}`}
                                        </SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>

                        <div className="space-y-2">
                            <label className="text-sm font-medium text-gray-700">Search</label>
                            <Input
                                type="text"
                                placeholder="Search events..."
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                className="w-full"
                            />
                        </div>
                    </div>

                    {/* Data Table */}
                    <div className="rounded-lg border border-gray-200 bg-white">
                        <Table>
                            <TableHeader>
                                <TableRow className="bg-gray-50 hover:bg-gray-50">
                                    <TableHead onClick={() => handleSort("country")} className="cursor-pointer select-none">
                                        Country
                                        {sortBy === "country" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                    <TableHead>First Disaster</TableHead>
                                    <TableHead onClick={() => handleSort("startDateA")} className="cursor-pointer select-none">
                                        Date
                                        {sortBy === "startDateA" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                    <TableHead onClick={() => handleSort("daysBetween")} className="cursor-pointer select-none">
                                        Days Between
                                        {sortBy === "daysBetween" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                    <TableHead>Second Disaster</TableHead>
                                    <TableHead onClick={() => handleSort("startDateB")} className="cursor-pointer select-none">
                                        Date
                                        {sortBy === "startDateB" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {paginatedCorrelations.length === 0 ? (
                                    <TableRow>
                                        <TableCell colSpan={6} className="text-center text-gray-500 py-8">
                                            <div className="flex flex-col items-center gap-2">
                                                <AlertTriangle className="h-8 w-8 text-gray-400" />
                                                <p>No correlations found</p>
                                                <p className="text-sm">Try adjusting the filters to see more results</p>
                                            </div>
                                        </TableCell>
                                    </TableRow>
                                ) : (
                                    paginatedCorrelations.map((correlation) => (
                                        <TableRow key={correlation.id} className="hover:bg-gray-50">
                                            <TableCell>
                                                <div className="flex items-center gap-2">
                                                    <MapPin className="h-4 w-4 text-gray-400" />
                                                    <span className="font-medium">{correlation.country}</span>
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <div className="space-y-1">
                                                    <Badge
                                                        variant="outline"
                                                        className={getDisasterTypeColor(correlation.disasterTypeA)}
                                                    >
                                                        {correlation.disasterTypeA}
                                                    </Badge>
                                                    <div className="text-sm">{correlation.eventNameA}</div>
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex items-center gap-2 text-sm text-gray-600">
                                                    <Calendar className="h-4 w-4" />
                                                    {new Date(correlation.startDateA).toLocaleDateString()}
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex items-center justify-center">
                                                    <Badge
                                                        variant="outline"
                                                        className="bg-gray-100 text-gray-700 border-gray-200"
                                                    >
                                                        {correlation.daysBetween === 0 ? (
                                                            "Same day"
                                                        ) : (
                                                            <div className="flex items-center gap-1">
                                                                <Clock className="h-3 w-3" />
                                                                {correlation.daysBetween} day{correlation.daysBetween === 1 ? "" : "s"}
                                                            </div>
                                                        )}
                                                    </Badge>
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <div className="space-y-1">
                                                    <Badge
                                                        variant="outline"
                                                        className={getDisasterTypeColor(correlation.disasterTypeB)}
                                                    >
                                                        {correlation.disasterTypeB}
                                                    </Badge>
                                                    <div className="text-sm">{correlation.eventNameB}</div>
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex items-center gap-2 text-sm text-gray-600">
                                                    <Calendar className="h-4 w-4" />
                                                    {new Date(correlation.startDateB).toLocaleDateString()}
                                                </div>
                                            </TableCell>
                                        </TableRow>
                                    ))
                                )}
                            </TableBody>
                        </Table>
                    </div>

                    {/* Pagination Controls */}
                    {totalResults > itemsPerPage && (
                        <div className="flex items-center justify-between pt-4">
                            <div className="text-sm text-gray-500">
                                Showing {((currentPage - 1) * itemsPerPage) + 1} to {Math.min(currentPage * itemsPerPage, totalResults)} of {totalResults} results
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
                                    disabled={currentPage * itemsPerPage >= totalResults}
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