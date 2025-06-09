"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { TabsContent } from "@/components/ui/tabs"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { ChevronUp, ChevronDown, Flame, Zap, CloudRain, Waves, Mountain } from "lucide-react"

interface Hotspot {
    country: string
    region: string
    numOccurrences: number
    totalImpactScore: number
    rank: number
}

const disasterTypes = [
    { key: "earthquake", label: "Earthquake", icon: Zap },
    { key: "wildfire", label: "Wildfire", icon: Flame },
    { key: "flood", label: "Flood", icon: Waves },
    { key: "storm", label: "Storm", icon: CloudRain },
    { key: "volcanic_activity", label: "Volcanic Activity", icon: Mountain },
]

// Mock data for each disaster type
const mockHotspots: Record<string, Hotspot[]> = {
    earthquake: [
        { country: "Japan", region: "Tohoku", numOccurrences: 12, totalImpactScore: 3200, rank: 1 },
        { country: "Indonesia", region: "Sumatra", numOccurrences: 10, totalImpactScore: 2900, rank: 2 },
        { country: "Chile", region: "Valparaiso", numOccurrences: 8, totalImpactScore: 2100, rank: 3 },
        { country: "Turkey", region: "Eastern Anatolia", numOccurrences: 7, totalImpactScore: 1800, rank: 4 },
        { country: "United States", region: "California", numOccurrences: 6, totalImpactScore: 1500, rank: 5 },
    ],
    wildfire: [
        { country: "Australia", region: "New South Wales", numOccurrences: 15, totalImpactScore: 4100, rank: 1 },
        { country: "United States", region: "California", numOccurrences: 13, totalImpactScore: 3900, rank: 2 },
        { country: "Brazil", region: "Amazonas", numOccurrences: 9, totalImpactScore: 2500, rank: 3 },
        { country: "Canada", region: "British Columbia", numOccurrences: 7, totalImpactScore: 1800, rank: 4 },
        { country: "Portugal", region: "Leiria", numOccurrences: 6, totalImpactScore: 1200, rank: 5 },
    ],
    flood: [
        { country: "Bangladesh", region: "Dhaka", numOccurrences: 14, totalImpactScore: 5200, rank: 1 },
        { country: "India", region: "Assam", numOccurrences: 12, totalImpactScore: 4800, rank: 2 },
        { country: "China", region: "Sichuan", numOccurrences: 10, totalImpactScore: 3500, rank: 3 },
        { country: "United States", region: "Louisiana", numOccurrences: 8, totalImpactScore: 2100, rank: 4 },
        { country: "Nigeria", region: "Lagos", numOccurrences: 7, totalImpactScore: 1700, rank: 5 },
    ],
    storm: [
        { country: "Philippines", region: "Luzon", numOccurrences: 16, totalImpactScore: 4300, rank: 1 },
        { country: "United States", region: "Florida", numOccurrences: 13, totalImpactScore: 3900, rank: 2 },
        { country: "Mexico", region: "Yucatan", numOccurrences: 9, totalImpactScore: 2500, rank: 3 },
        { country: "Vietnam", region: "Central Coast", numOccurrences: 8, totalImpactScore: 2100, rank: 4 },
        { country: "Japan", region: "Okinawa", numOccurrences: 7, totalImpactScore: 1700, rank: 5 },
    ],
    volcanic_activity: [
        { country: "Indonesia", region: "Java", numOccurrences: 11, totalImpactScore: 3100, rank: 1 },
        { country: "Iceland", region: "Reykjanes", numOccurrences: 8, totalImpactScore: 2500, rank: 2 },
        { country: "Italy", region: "Sicily", numOccurrences: 7, totalImpactScore: 2200, rank: 3 },
        { country: "Japan", region: "Kyushu", numOccurrences: 6, totalImpactScore: 1800, rank: 4 },
        { country: "Ecuador", region: "Galapagos", numOccurrences: 5, totalImpactScore: 1200, rank: 5 },
    ],
}

export function HotspotsTab() {
    const [selectedType, setSelectedType] = useState<string>(disasterTypes[0].key)
    const [sortBy, setSortBy] = useState<keyof Hotspot>("rank")
    const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc")

    const hotspots = mockHotspots[selectedType] || []
    const sortedHotspots = [...hotspots].sort((a, b) => {
        let aValue = a[sortBy]
        let bValue = b[sortBy]
        if (typeof aValue === "string" && typeof bValue === "string") {
            aValue = aValue.toLowerCase()
            bValue = bValue.toLowerCase()
        }
        if (aValue < bValue) return sortDirection === "asc" ? -1 : 1
        if (aValue > bValue) return sortDirection === "asc" ? 1 : -1
        return 0
    })

    const handleSort = (column: keyof Hotspot) => {
        if (sortBy === column) {
            setSortDirection(sortDirection === "asc" ? "desc" : "asc")
        } else {
            setSortBy(column)
            setSortDirection("asc")
        }
    }

    const Icon = disasterTypes.find(dt => dt.key === selectedType)?.icon

    return (
        <TabsContent value="hotspots">
            <Card>
                <CardHeader className="pb-3">
                    <div className="flex items-center gap-2">
                        {Icon && <Icon className="h-6 w-6 text-orange-500" />}
                        <div>
                            <CardTitle className="text-2xl">Disaster Hotspots</CardTitle>
                            <CardDescription className="mt-1.5">
                                Top 5 regions per disaster type, ranked by frequency and impact
                            </CardDescription>
                        </div>
                    </div>
                </CardHeader>
                <CardContent className="space-y-4">
                    <Tabs value={selectedType} onValueChange={setSelectedType} className="w-full">
                        <TabsList className="mb-4 grid grid-cols-5 gap-2">
                            {disasterTypes.map(dt => (
                                <TabsTrigger key={dt.key} value={dt.key} className="flex items-center gap-2">
                                    <dt.icon className="h-4 w-4" />
                                    {dt.label}
                                </TabsTrigger>
                            ))}
                        </TabsList>
                    </Tabs>
                    <div className="rounded-lg border border-gray-200 bg-white">
                        <Table>
                            <TableHeader>
                                <TableRow className="bg-gray-50 hover:bg-gray-50">
                                    <TableHead onClick={() => handleSort("rank")} className="cursor-pointer select-none text-center">
                                        Rank
                                        {sortBy === "rank" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                    <TableHead onClick={() => handleSort("country")} className="cursor-pointer select-none text-center">
                                        Country
                                        {sortBy === "country" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                    <TableHead onClick={() => handleSort("region")} className="cursor-pointer select-none text-center">
                                        Region
                                        {sortBy === "region" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                    <TableHead onClick={() => handleSort("numOccurrences")} className="cursor-pointer select-none text-center">
                                        # Occurrences
                                        {sortBy === "numOccurrences" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                    <TableHead onClick={() => handleSort("totalImpactScore")} className="cursor-pointer select-none text-center">
                                        Total Impact Score
                                        {sortBy === "totalImpactScore" && (sortDirection === "asc" ? <ChevronUp className="inline h-4 w-4 ml-1" /> : <ChevronDown className="inline h-4 w-4 ml-1" />)}
                                    </TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {sortedHotspots.length === 0 ? (
                                    <TableRow>
                                        <TableCell colSpan={5} className="text-center text-gray-500 py-8">
                                            No hotspots found for this disaster type.
                                        </TableCell>
                                    </TableRow>
                                ) : (
                                    sortedHotspots.map((hotspot) => (
                                        <TableRow key={hotspot.rank} className="hover:bg-gray-50">
                                            <TableCell className="text-center">
                                                <Badge variant="outline" className="bg-orange-50 text-orange-700 border-orange-200 font-bold">
                                                    {hotspot.rank}
                                                </Badge>
                                            </TableCell>
                                            <TableCell className="text-center">{hotspot.country}</TableCell>
                                            <TableCell className="text-center">{hotspot.region}</TableCell>
                                            <TableCell className="text-center font-medium">{hotspot.numOccurrences}</TableCell>
                                            <TableCell className="text-center font-medium">{hotspot.totalImpactScore.toLocaleString(undefined, { maximumFractionDigits: 1 })}</TableCell>
                                        </TableRow>
                                    ))
                                )}
                            </TableBody>
                        </Table>
                    </div>
                </CardContent>
            </Card>
        </TabsContent>
    )
} 