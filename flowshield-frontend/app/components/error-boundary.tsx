"use client"

import { Component, ErrorInfo, ReactNode } from "react"
import { AlertTriangle } from "lucide-react"
import { Button } from "@/components/ui/button"

interface Props {
    children: ReactNode
}

interface State {
    hasError: boolean
    error: Error | null
}

export class ErrorBoundary extends Component<Props, State> {
    public state: State = {
        hasError: false,
        error: null,
    }

    public static getDerivedStateFromError(error: Error): State {
        return { hasError: true, error }
    }

    public componentDidCatch(error: Error, errorInfo: ErrorInfo) {
        console.error("Uncaught error:", error, errorInfo)
    }

    public render() {
        if (this.state.hasError) {
            return (
                <div className="min-h-screen flex items-center justify-center bg-background">
                    <div className="max-w-md w-full p-6 bg-card rounded-lg shadow-lg">
                        <div className="flex items-center space-x-3 mb-4">
                            <AlertTriangle className="h-6 w-6 text-orange-500" />
                            <h2 className="text-xl font-semibold">Something went wrong</h2>
                        </div>
                        <p className="text-muted-foreground mb-4">
                            {this.state.error?.message || "An unexpected error occurred"}
                        </p>
                        <Button
                            onClick={() => {
                                this.setState({ hasError: false, error: null })
                                window.location.reload()
                            }}
                        >
                            Try again
                        </Button>
                    </div>
                </div>
            )
        }

        return this.props.children
    }
} 