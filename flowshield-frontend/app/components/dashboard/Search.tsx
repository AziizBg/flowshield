import { Search as SearchIcon } from "lucide-react"
import { Input } from "../ui/input"
import { Card, CardContent } from "../ui/card"

interface SearchProps {
    value: string
    onChange: (value: string) => void
    placeholder?: string
}

export function Search({ value, onChange, placeholder = "Search events..." }: SearchProps) {
    return (
        <Card className="mb-6">
            <CardContent className="pt-4">
                <div className="relative">
                    <SearchIcon className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
                    <Input
                        placeholder={placeholder}
                        value={value}
                        onChange={(e) => onChange(e.target.value)}
                        className="pl-8"
                    />
                </div>
            </CardContent>
        </Card>
    )
} 