# current problems / todos

* file access can be temporarily restricted (for some reason it only happens when im trying to copy file with windows
  explorer (Ctrl+C - Ctrl+V in the same dir))
* more general: exception handling in filesystem interaction
* unable to delete file on windows despite `File.useLines` was used to open it and iteration over lines was finished
* directory deletion event may occur earlier than its content deletion events
* compare file attributes to skip some updates?