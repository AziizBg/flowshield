import { Card, CardContent, CardHeader, CardTitle } from "../ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../ui/select"

interface FiltersProps {
    selectedEventType: "all" | "earthquake" | "fire"
    selectedSeverity: "all" | "low" | "moderate" | "high"
    selectedTimeRange: "12h" | "6h" | "5h" | "3h" | "2h" | "1h" | "30m" | "1m"
    onEventTypeChange: (type: "all" | "earthquake" | "fire") => void
    onSeverityChange: (severity: "all" | "low" | "moderate" | "high") => void
    onTimeRangeChange: (range: "12h" | "6h" | "5h" | "3h" | "2h" | "1h" | "30m" | "1m") => void
}

export function Filters({
    selectedEventType,
    selectedSeverity,
    selectedTimeRange,
    onEventTypeChange,
    onSeverityChange,
    onTimeRangeChange,
}: FiltersProps) {
    return (
        <div className="flex flex-col gap-4 p-4 bg-white rounded-lg shadow-sm border border-gray-100">
            <div className="space-y-2">
                <h3 className="text-sm font-medium text-gray-700">Event Type</h3>
                <Select value={selectedEventType} onValueChange={onEventTypeChange}>
                    <SelectTrigger className="w-full bg-gray-50 hover:bg-gray-100 transition-colors">
                        <SelectValue placeholder="Select event type" />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="all" className="hover:bg-blue-50">All Events</SelectItem>
                        <SelectItem value="earthquake" className="hover:bg-blue-50">Earthquakes</SelectItem>
                        <SelectItem value="fire" className="hover:bg-blue-50">Fires</SelectItem>
                    </SelectContent>
                </Select>
            </div>

            <div className="space-y-2">
                <h3 className="text-sm font-medium text-gray-700">Severity</h3>
                <Select value={selectedSeverity} onValueChange={onSeverityChange}>
                    <SelectTrigger className="w-full bg-gray-50 hover:bg-gray-100 transition-colors">
                        <SelectValue placeholder="Select severity" />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="all" className="hover:bg-blue-50">All Severities</SelectItem>
                        <SelectItem value="low" className="hover:bg-blue-50">Low</SelectItem>
                        <SelectItem value="moderate" className="hover:bg-blue-50">Moderate</SelectItem>
                        <SelectItem value="high" className="hover:bg-blue-50">High</SelectItem>
                    </SelectContent>
                </Select>
            </div>

            <div className="space-y-2">
                <h3 className="text-sm font-medium text-gray-700">Time Range</h3>
                <Select value={selectedTimeRange} onValueChange={onTimeRangeChange}>
                    <SelectTrigger className="w-full bg-gray-50 hover:bg-gray-100 transition-colors">
                        <SelectValue placeholder="Select time range" />
                    </SelectTrigger>
                    <SelectContent>
                        <SelectItem value="12h" className="hover:bg-blue-50">Last 12 Hours</SelectItem>
                        <SelectItem value="6h" className="hover:bg-blue-50">Last 6 Hours</SelectItem>
                        <SelectItem value="5h" className="hover:bg-blue-50">Last 5 Hours</SelectItem>
                        <SelectItem value="3h" className="hover:bg-blue-50">Last 3 Hours</SelectItem>
                        <SelectItem value="2h" className="hover:bg-blue-50">Last 2 Hours</SelectItem>
                        <SelectItem value="1h" className="hover:bg-blue-50">Last Hour</SelectItem>
                        <SelectItem value="30m" className="hover:bg-blue-50">Last 30 Minutes</SelectItem>
                        <SelectItem value="1m" className="hover:bg-blue-50">Last Minute</SelectItem>
                    </SelectContent>
                </Select>
            </div>
        </div>
    )
} 