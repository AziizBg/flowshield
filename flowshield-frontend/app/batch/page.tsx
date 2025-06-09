"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Clock, AlertTriangle, Calendar, MapPin, BarChart2, Globe } from "lucide-react"
import Link from "next/link"
import { Header } from "../components/dashboard/Header"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

export default function BatchDashboard() {
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
                        <Card>
                            <CardHeader>
                                <CardTitle>Anomalous Disasters</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-center text-gray-500">
                                    <p className="text-lg">Anomaly detection analysis coming soon...</p>
                                    <p className="mt-2">This view will show disaster events that significantly exceed normal thresholds for magnitude, deaths, or affected population.</p>
                                </div>
                            </CardContent>
                        </Card>
                    </TabsContent>

                    <TabsContent value="correlations">
                        <Card>
                            <CardHeader>
                                <CardTitle>Temporally Correlated Disasters</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-center text-gray-500">
                                    <p className="text-lg">Temporal correlation analysis coming soon...</p>
                                    <p className="mt-2">This view will show pairs of different disaster types that occurred within 7 days of each other in the same country.</p>
                                </div>
                            </CardContent>
                        </Card>
                    </TabsContent>

                    <TabsContent value="hotspots">
                        <Card>
                            <CardHeader>
                                <CardTitle>Disaster Hotspots</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-center text-gray-500">
                                    <p className="text-lg">Hotspot analysis coming soon...</p>
                                    <p className="mt-2">This view will show the top 5 hotspot regions for each major disaster type, based on frequency and impact.</p>
                                </div>
                            </CardContent>
                        </Card>
                    </TabsContent>

                    <TabsContent value="vulnerability">
                        <Card>
                            <CardHeader>
                                <CardTitle>Country Vulnerability Index</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="text-center text-gray-500">
                                    <p className="text-lg">Vulnerability analysis coming soon...</p>
                                    <p className="mt-2">This view will show countries ranked by their disaster vulnerability, considering fatalities, economic damage, and recovery time.</p>
                                </div>
                            </CardContent>
                        </Card>
                    </TabsContent>

                    <TabsContent value="summaries">
                        <Card>
                            <CardHeader>
                                <CardTitle>Disaster Summaries</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                    {["Earthquake", "Wildfire", "Flood", "Storm", "Volcanic Activity"].map((type) => (
                                        <Card key={type}>
                                            <CardHeader>
                                                <CardTitle className="text-lg">{type} Summary</CardTitle>
                                            </CardHeader>
                                            <CardContent>
                                                <div className="text-center text-gray-500">
                                                    <p>{type} statistics coming soon...</p>
                                                </div>
                                            </CardContent>
                                        </Card>
                                    ))}
                                </div>
                            </CardContent>
                        </Card>
                    </TabsContent>
                </Tabs>
            </div>
        </div>
    )
} 