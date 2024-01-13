rootDir <- "docDB"

main <- function()
{
  # Test make dir
  configDB(rootDir, "")
  
  # Test get extension PNG
  print(getExtension("CampusAtNight.png"))
  
  # Test get file stem CampusAtNight.png
  print(getFileStem("CampusAtNight.png"))
  
  # Test get file stem CampusAtNight.abc
  print(getFileStem("CampusAtNight.abc.png"))
  
  # Test get  dir JPG
  print(getObjPath(rootDir, "JPEG"))
  
  # Test get dir PDF
  print(getObjPath(rootDir, "PDF"))
  
  # Add files to docDB for bellow test
  file.create(file.path(rootDir, "test1.pdf"))
  file.create(file.path(rootDir, "test2.png"))
  file.create(file.path(rootDir, "test3.jpg"))
  file.create(file.path(rootDir, "test4.jpeg"))
  file.create(file.path(rootDir, "test5.doc"))
  file.create(file.path(rootDir, "test6.docx"))
  
  # Test copy from docDB to copy
  storeObjs(rootDir, "copy")
  
  # Test clear copy.PDF
  clearDB("copy/PDF")
}

# Create a folder under path
# root: name of the folder
# path: path to create the folder
configDB <- function(root, path = "")
{
  # Get dir to be created
  if (path != "") {
    newDir <- paste(path, root, sep = "/")
  } else {
    newDir <- root
  }
  
  # Check if the new dir already exists. If not then create the new dir.
  if (!dir.exists(newDir)) 
  {
    dir.create(newDir)
  }
}

# Get extension of a file
# fileName: name of the file to get the extension
# return: file extension
getExtension <- function(fileName)
{
  # Get the last element after spliting string
  names <- strsplit(fileName, "\\.")[[1]]
  return(toupper(names[length(names)]))
}

# Get stem of a file
# fileName: name of the file to get the extension
# return: file stem
getFileStem <- function(fileName)
{
  # Get all elements before the last dot. Then concatenate them.
  names <- strsplit(fileName, "\\.")[[1]]
  return(paste(names[1:length(names) - 1], collapse = "."))
}

# Get correct path of type folder
# root: parent folder of the type dir
# tag: extension
# return: correct path
getObjPath <- function(root, tag)
{
  if (tag == "JPEG") {
    tag <- "JPG"
  }
  if (tag == "DOCX") {
    tag <- "DOC"
  }
  return(paste(root, tag, sep = "/"))
}

# Copy every file in folder to root
# folder: dir to copy from
# root: dir to copy to
storeObjs <- function(folder, root, verbose = TRUE)
{
  # Create root if doesn't exist.
  rootPath <- strsplit(root, "/")[[1]]
  curr <- ""
  for (i in 1:length(rootPath)) {
    configDB(rootPath[i], curr)
    curr <- file.path(curr, rootPath[i])
  }
  
  # Stop if folder is contained in root
  if (file.exists(file.path(root, folder))) {
    return()
  }
  
  # Get all files in folder
  files <- list.files(path = folder)
  
  for (file in files) {
    if (!hasExtension(file)) {
      next
    }
    # Get extension and derive dir to copy to
    extension <- getExtension(file)
    objPath <- getObjPath(root, extension)
    
    # Create type folder
    paths <- strsplit(objPath, "/")[[1]]
    configDB(paths[length(paths)], paste(paths[1:length(paths) - 1], sep = "/"))
    
    # Copy to type folder
    from <- paste(folder, file, sep = "/")
    to <- paste0(objPath, "/")
    file.copy(from, to)
    
    # Copy message
    print(paste("Copying", getFileStem(file), "to folder", paths[length(paths)]))
  }
}

# Remove all files and folders in root
# root: dir to be cleared
clearDB <- function(root) {
  files <- list.files(root, include.dirs = TRUE)
  for (file in files) {
    unlink(file.path(root, file), recursive = TRUE)
  }
  print(paste(root, "cleared"))
}

# Check if the file has an extension
# fileName: file to check
# return: boolean
hasExtension <- function(fileName)
{
  names <- strsplit(fileName, "\\.")[[1]]
  if (length(names) == 1) {
    return(FALSE)
  } else {
    return(TRUE)
  }
}

#########################################################


main()
# quit()

