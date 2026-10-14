<#
.SYNOPSIS
    Generates every raster brand asset from one description of the mark.

.DESCRIPTION
    The mark is three streams narrowing into a single conflated value: two white
    bars and an accent tip inside a rounded indigo square. It exists as SVG for
    the site (docs/assets/logo.svg) and is redrawn here for the formats that need
    a raster:

      installer/redis-conflated-pubsub.ico   embedded in the executable by
                                             build.rs and used by Inno Setup
      docs/assets/favicon.ico                site favicon
      docs/assets/apple-touch-icon.png       iOS home screen
      docs/assets/social-card.png            1200x630 link preview

    There is no image toolchain in CI, so the generated files are committed and
    this script is what regenerates them. Keep the geometry in step with
    docs/assets/logo.svg: it is the same mark, drawn twice because one side needs
    vectors and the other needs pixels.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File tools\make-icons.ps1
#>
[CmdletBinding()]
param(
    [string]$RepositoryRoot
)

$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Drawing

if (-not $RepositoryRoot) {
    $RepositoryRoot = Split-Path -Parent $PSScriptRoot
}

$indigo = [System.Drawing.Color]::FromArgb(255, 63, 81, 181)   # #3f51b5
$accent = [System.Drawing.Color]::FromArgb(255, 0, 188, 212)   # #00bcd4
$white = [System.Drawing.Color]::White

function New-RoundedSquarePath {
    param([single]$Size, [single]$Inset, [single]$Radius)

    $path = New-Object System.Drawing.Drawing2D.GraphicsPath
    $diameter = $Radius * 2
    $left = $Inset
    $top = $Inset
    $right = $Size - $Inset
    $bottom = $Size - $Inset
    $path.AddArc($left, $top, $diameter, $diameter, 180, 90)
    $path.AddArc($right - $diameter, $top, $diameter, $diameter, 270, 90)
    $path.AddArc($right - $diameter, $bottom - $diameter, $diameter, $diameter, 0, 90)
    $path.AddArc($left, $bottom - $diameter, $diameter, $diameter, 90, 90)
    $path.CloseFigure()
    return $path
}

function New-MarkBitmap {
    param([int]$Size)

    $bitmap = New-Object System.Drawing.Bitmap($Size, $Size, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
    $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
    $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $graphics.Clear([System.Drawing.Color]::Transparent)

    # Everything is expressed in the 64-unit grid of the SVG, then scaled.
    $scale = $Size / 64.0
    $background = New-RoundedSquarePath -Size $Size -Inset ([single](2 * $scale)) -Radius ([single](15 * $scale))
    $backgroundBrush = New-Object System.Drawing.SolidBrush($indigo)
    $graphics.FillPath($backgroundBrush, $background)
    $backgroundBrush.Dispose()
    $background.Dispose()

    $bars = @(
        @{ X = 12; Y = 16; Width = 40; Color = $white;                       Opacity = 1.0 },
        @{ X = 18; Y = 28; Width = 28; Color = $white;                       Opacity = 0.72 },
        @{ X = 24; Y = 40; Width = 16; Color = $accent;                      Opacity = 1.0 }
    )

    foreach ($bar in $bars) {
        $brush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(
            [int](255 * $bar.Opacity), $bar.Color))
        $rect = New-Object System.Drawing.RectangleF(
            [single]($bar.X * $scale), [single]($bar.Y * $scale),
            [single]($bar.Width * $scale), [single](8 * $scale))
        $radius = [single](4 * $scale)

        $barPath = New-Object System.Drawing.Drawing2D.GraphicsPath
        $d = $radius * 2
        $barPath.AddArc($rect.Left, $rect.Top, $d, $d, 180, 90)
        $barPath.AddArc($rect.Right - $d, $rect.Top, $d, $d, 270, 90)
        $barPath.AddArc($rect.Right - $d, $rect.Bottom - $d, $d, $d, 0, 90)
        $barPath.AddArc($rect.Left, $rect.Bottom - $d, $d, $d, 90, 90)
        $barPath.CloseFigure()
        $graphics.FillPath($brush, $barPath)
        $barPath.Dispose()
        $brush.Dispose()
    }

    $graphics.Dispose()
    return $bitmap
}

function Write-Ico {
    param([int[]]$Sizes, [string]$Path)

    $images = @()
    foreach ($size in $Sizes) {
        $bitmap = New-MarkBitmap -Size $size
        $stream = New-Object System.IO.MemoryStream
        $bitmap.Save($stream, [System.Drawing.Imaging.ImageFormat]::Png)
        $images += [pscustomobject]@{ Size = $size; Bytes = $stream.ToArray() }
        $stream.Dispose()
        $bitmap.Dispose()
    }

    # ICONDIR + ICONDIRENTRY[] + PNG payloads. PNG entries are valid since Vista
    # and keep the 256 px frame from bloating the executable.
    $output = New-Object System.IO.MemoryStream
    $writer = New-Object System.IO.BinaryWriter($output)
    $writer.Write([uint16]0)
    $writer.Write([uint16]1)
    $writer.Write([uint16]$images.Count)

    $offset = 6 + (16 * $images.Count)
    foreach ($image in $images) {
        $dimension = if ($image.Size -ge 256) { 0 } else { $image.Size }
        $writer.Write([byte]$dimension)
        $writer.Write([byte]$dimension)
        $writer.Write([byte]0)
        $writer.Write([byte]0)
        $writer.Write([uint16]1)
        $writer.Write([uint16]32)
        $writer.Write([uint32]$image.Bytes.Length)
        $writer.Write([uint32]$offset)
        $offset += $image.Bytes.Length
    }
    foreach ($image in $images) { $writer.Write($image.Bytes) }
    $writer.Flush()

    [System.IO.File]::WriteAllBytes($Path, $output.ToArray())
    $writer.Dispose()
    $output.Dispose()
    Write-Host ("  {0} ({1} bytes, sizes: {2})" -f (Split-Path -Leaf $Path), (Get-Item $Path).Length, ($Sizes -join '/'))
}

function Write-Png {
    param([System.Drawing.Bitmap]$Bitmap, [string]$Path)

    $Bitmap.Save($Path, [System.Drawing.Imaging.ImageFormat]::Png)
    $Bitmap.Dispose()
    Write-Host ("  {0} ({1} bytes)" -f (Split-Path -Leaf $Path), (Get-Item $Path).Length)
}

function New-SocialCard {
    param([int]$Width = 1200, [int]$Height = 630)

    $bitmap = New-Object System.Drawing.Bitmap($Width, $Height, [System.Drawing.Imaging.PixelFormat]::Format32bppArgb)
    $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
    $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $graphics.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::ClearTypeGridFit

    # Same gradient family as the site's dark surface.
    $gradient = New-Object System.Drawing.Drawing2D.LinearGradientBrush(
        (New-Object System.Drawing.Point(0, 0)),
        (New-Object System.Drawing.Point($Width, $Height)),
        [System.Drawing.Color]::FromArgb(255, 26, 35, 126),
        [System.Drawing.Color]::FromArgb(255, 63, 81, 181))
    $graphics.FillRectangle($gradient, 0, 0, $Width, $Height)

    # The mark, drawn large and without its own background: the card already is
    # the indigo surface. Bars are centred on the same axis as the SVG mark, so
    # they read as streams converging instead of a bar chart.
    $axis = 330
    $barWidths = @(300, 210, 130)
    $y = 196
    foreach ($barWidth in $barWidths) {
        $color = if ($barWidth -eq 130) { $accent } else { $white }
        $opacity = if ($barWidth -eq 210) { 0.72 } else { 1.0 }
        $brush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb([int](255 * $opacity), $color))
        $height = 56
        $radius = 28
        $x = $axis - ($barWidth / 2)
        $rect = New-Object System.Drawing.RectangleF($x, $y, $barWidth, $height)
        $path = New-Object System.Drawing.Drawing2D.GraphicsPath
        $d = $radius * 2
        $path.AddArc($rect.Left, $rect.Top, $d, $d, 180, 90)
        $path.AddArc($rect.Right - $d, $rect.Top, $d, $d, 270, 90)
        $path.AddArc($rect.Right - $d, $rect.Bottom - $d, $d, $d, 0, 90)
        $path.AddArc($rect.Left, $rect.Bottom - $d, $d, $d, 90, 90)
        $path.CloseFigure()
        $graphics.FillPath($brush, $path)
        $path.Dispose()
        $brush.Dispose()
        $y += 88
    }

    $titleFont = New-Object System.Drawing.Font('Segoe UI', 62, [System.Drawing.FontStyle]::Bold, [System.Drawing.GraphicsUnit]::Pixel)
    $taglineFont = New-Object System.Drawing.Font('Segoe UI', 30, [System.Drawing.FontStyle]::Regular, [System.Drawing.GraphicsUnit]::Pixel)
    $metaFont = New-Object System.Drawing.Font('Consolas', 22, [System.Drawing.FontStyle]::Regular, [System.Drawing.GraphicsUnit]::Pixel)
    $textBrush = New-Object System.Drawing.SolidBrush($white)
    $mutedBrush = New-Object System.Drawing.SolidBrush([System.Drawing.Color]::FromArgb(210, 255, 255, 255))

    $graphics.DrawString('Conflated Pub/Sub', $titleFont, $textBrush, 512, 196)
    $graphics.DrawString('Redis and Core NATS Pub/Sub relay', $taglineFont, $mutedBrush, 516, 286)
    $graphics.DrawString('with per-output conflation and deduplication', $taglineFont, $mutedBrush, 516, 326)
    $graphics.DrawString('github.com/rspadim/redis-conflated-pubsub', $metaFont, $mutedBrush, 516, 402)

    $titleFont.Dispose(); $taglineFont.Dispose(); $metaFont.Dispose()
    $textBrush.Dispose(); $mutedBrush.Dispose(); $gradient.Dispose()
    $graphics.Dispose()
    return $bitmap
}

$installerIcon = Join-Path $RepositoryRoot 'installer\redis-conflated-pubsub.ico'
$siteAssets = Join-Path $RepositoryRoot 'docs\assets'

Write-Host 'brand assets:'
Write-Ico -Sizes @(16, 20, 24, 32, 40, 48, 64, 128, 256) -Path $installerIcon
Write-Ico -Sizes @(16, 32, 48) -Path (Join-Path $siteAssets 'favicon.ico')
Write-Png -Bitmap (New-MarkBitmap -Size 180) -Path (Join-Path $siteAssets 'apple-touch-icon.png')
Write-Png -Bitmap (New-MarkBitmap -Size 512) -Path (Join-Path $siteAssets 'icon-512.png')
Write-Png -Bitmap (New-SocialCard) -Path (Join-Path $siteAssets 'social-card.png')
