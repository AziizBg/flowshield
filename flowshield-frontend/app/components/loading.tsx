import { RefreshCw } from "lucide-react"

interface LoadingProps {
    message?: string
    className?: string
}

export function Loading({ message = "Loading...", className = "" }: LoadingProps) {
    return (
        <div className={`p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg ${className}`}>
            <div className="flex items-center space-x-2">
                <RefreshCw className="h-4 w-4 animate-spin text-blue-600" />
                <span className="text-blue-600 dark:text-blue-400">{message}</span>
            </div>
        </div>
    )
} 