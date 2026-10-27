<#
.SYNOPSIS
Creates a GeoPackage index of geotagged photos for matching directories.

.DESCRIPTION
Runs QGIS's native:importphotos algorithm recursively for each matching
folder. Existing photo_locations.gpkg indexes are skipped. New indexes are
written to a temporary file and moved into place only after successful
completion. The script does not modify source photos.

.PARAMETER Pattern
A directory path or a wildcard pattern matched against immediate
subdirectories of the current directory. Use '*' to match all immediate
subdirectories.

.EXAMPLE
.\index-geo-photos.ps1 '2024*'

.EXAMPLE
.\index-geo-photos.ps1 '\\server\share\photos'

.EXAMPLE
.\index-geo-photos.ps1 '*'

_NOTES
_codex-6.1-sol-medium on behalf of mhwilkie_
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Pattern
)

if ([string]::IsNullOrWhiteSpace($Pattern)) {
    Write-Host "Usage: .\index-geo-photos.ps1 <directory-path-or-pattern>"
    Write-Host "Example: .\index-geo-photos.ps1 '2024*'"
    Write-Host "Example: .\index-geo-photos.ps1 '\\server\share\photos'"
    Write-Host "Use '*' to process every immediate subdirectory of the current directory."
    return
}

$root = (Get-Location).Path
$env:QT_QPA_PLATFORM = "offscreen"
$folders = @()

if (Test-Path -LiteralPath $Pattern -PathType Container) {
    $folders = @(Get-Item -LiteralPath $Pattern)
} else {
    $folders = @(Get-ChildItem -LiteralPath $root -Directory | Where-Object { $_.Name -like $Pattern })
}

if ($folders.Count -eq 0) {
    Write-Host "No directories match pattern: $Pattern"
    return
}

$total = $folders.Count
$index = 0

foreach ($directory in $folders) {
    $index++
    $folder = $directory.FullName
    $output = Join-Path $folder "photo_locations.gpkg"
    $status = "Indexing"

    if (Test-Path -LiteralPath $output -PathType Leaf) {
        $status = "Skipping existing index"
        Write-Progress -Activity "Indexing geo photos ($index of $total)" -Status "$status`: $($directory.Name)" -PercentComplete (($index / $total) * 100)
        Write-Host "Skipping: $folder"
        continue
    }

    Write-Progress -Activity "Indexing geo photos ($index of $total)" -Status "$status`: $($directory.Name)" -PercentComplete ((($index - 1) / $total) * 100)

    # Publish the final filename only after successful completion.
    $temporary = Join-Path $folder ("photo_locations_" + [guid]::NewGuid() + ".gpkg")

    qgis_process --skip-loading-plugins run native:importphotos -- `
        "FOLDER=$folder" `
        "RECURSIVE=true" `
        "OUTPUT=$temporary"

    if ($LASTEXITCODE -ne 0) {
        throw "Photo indexing failed: $folder"
    }

    Move-Item -LiteralPath $temporary -Destination $output
    Write-Host "Indexed: $folder"
}

Write-Progress -Activity "Indexing geo photos" -Completed
Write-Host "Finished: checked $total matching directories."
