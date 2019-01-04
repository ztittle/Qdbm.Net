#!/bin/sh
dotnet build -c Release
dotnet pack -c Release --version-suffix alpha$(date +%s)
dotnet nuget push $(ls -Art Qdbm.Net/bin/Release/*.nupkg | tail -n 1) -s http://10.0.10.3/nexus/repository/nuget-hosted/ -k $RI_NUGET_KEY

