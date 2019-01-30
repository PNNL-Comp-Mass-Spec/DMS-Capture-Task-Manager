# DMS Capture Task Manager

The DMS Capture Task Manager is a part of PRISM, the
Proteomics Research Information and Management System.
The capture manager processes instrument data files, copying them
from the instrument to storage servers, validating the data, 
creating QC graphics, and pushing the data into MyEMSL.
Plugin DLLs implement the processing details for each tool.

## Plugins

| DMS Capture Step Tool | Description | Plugin Folder | Plugin DLL | 
|-----------------------|-------------|---------------|------------|
| ArchiveStatusCheck | Verify that all of the ingest jobs associated with the given dataset are complete | ArchiveStatusCheckPlugin | ArchiveStatusCheckPlugin.dll |
| ArchiveUpdate | Create specific analysis results folder in dataset folder in archive and copy contents of results folder in storage to it | DatasetArchivePlugin | DatasetArchivePlugin.dll |
| ArchiveVerify | Verify that checksums reported by MyEMSL match those of the ingested data | ArchiveVerifyPlugin | ArchiveVerifyPlugin.dll |
| DatasetArchive | Create dataset folder on archive and copy everything from storage dataset folder into it | DatasetArchivePlugin | DatasetArchivePlugin.dll |
| DatasetCapture | Create dataset folder on storage server and copy instrument data into it | CaptureToolPlugin | CaptureToolPlugin.dll |
| DatasetInfo | Create QC graphics | DatasetInfoPlugin | DatasetInfoPlugin.dll |
| DatasetIntegrity | Makes sure that captured file is valid (not too small, required files/folders are present). For IMS08, converts the .D folder to .UIMF. For Agilent GC, converts the .D folder to CDF using OpenChrom | DatasetIntegrityPlugin | DatasetIntegrityPlugin.dll |
| DatasetQuality | Creates the metadata.xml file and runs Quameter | DatasetQualityPlugin | DatasetQualityPlugin.dll |
| ImsDeMultiplex | DeMux IMS data | ImsDemuxPlugin | ImsDemuxPlugin.dll |
| SourceFileRename | Put "x_" prefix on source files or source folders in the instrument source directory | SrcFileRenamePlugin | SrcFileRenamePlugin.dll |
	
## Contacts

Written by Matthew Monroe and Dave Clark for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov \
Website: https://panomics.pnl.gov/ or https://omics.pnl.gov

## License

The DMS Capture Manager is licensed under the 2-Clause BSD License; you may not use this file 
except in compliance with the License. You may obtain a copy of the License at 
https://opensource.org/licenses/BSD-2-Clause

Copyright 2018 Battelle Memorial Institute

RawFileReader reading tool. Copyright © 2016 by Thermo Fisher Scientific, Inc. All rights reserved.
