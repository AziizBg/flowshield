"use client"

import { Card, CardContent } from "@/components/ui/card"
import { Clock } from "lucide-react"
import Link from "next/link"
import { Header } from "../components/dashboard/Header"

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
                    historicalDataLink={
                        <Link
                            href="/"
                            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-50 text-blue-700 rounded-full transition-colors"
                        >
                            <Clock className="h-4 w-4" />
                            <span className="text-sm font-medium">Historical</span>
                        </Link>
                    }
                />

                <Card>
                    <CardContent className="p-6">
                        <div className="text-center text-gray-500">
                            <p className="text-lg">Historical data analysis view coming soon...</p>
                            <p className="mt-2">This view will provide insights and analysis of past events.</p>
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    )
} 