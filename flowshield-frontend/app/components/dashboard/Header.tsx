import { AlertTriangle, Activity, RefreshCw, Clock } from "lucide-react"
import { Badge } from "../ui/badge"
import { Button } from "../ui/button"

interface HeaderProps {
    useRealData: boolean
    isRealTime: boolean
    lastUpdated: Date | null
    isLoading: boolean
    onToggleDataMode: () => void
    onToggleRealTime: () => void
    onRefresh: () => void
}

export function Header({
    useRealData,
    isRealTime,
    lastUpdated,
    isLoading,
    onToggleDataMode,
    onToggleRealTime,
    onRefresh,
}: HeaderProps) {
    return (
        <div className="flex flex-col gap-4 p-4 bg-white rounded-lg shadow-sm border border-gray-100">
            <div className="flex items-center justify-between">
                <div className="space-y-1">
                    <h1 className="text-2xl font-bold text-gray-900">FlowShield Dashboard</h1>
                    <p className="text-sm text-gray-500">
                        Real-time monitoring of natural disasters and emergencies
                    </p>
                </div>
                <div className="flex items-center gap-2">
                    <div className="flex items-center gap-1.5 px-3 py-1.5 bg-green-50 rounded-full">
                        <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
                        <span className="text-sm font-medium text-green-700">Live</span>
                    </div>
                </div>
            </div>
            {lastUpdated && (
                <div className="flex items-center gap-2 text-sm text-gray-500">
                    <Clock className="h-4 w-4" />
                    <span>Last updated: {lastUpdated.toLocaleString()}</span>
                </div>
            )}
        </div>
    )
} 