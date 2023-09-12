# how to use

* launch: '\<binary\>' without any arguments
* stupid-REPL:
    * "exit" to exit
    * "search \<token\>" to search for token
    * "add \<path\>" to add path to watch
    * "remove \<path\>" to remove watch
    * any other string treated as single token

# current problems / todos

* java file watcher locks watched dir parent on windows, so you are unable to delete directory that have another watched
  directory inside
* compare file attributes to skip some updates?
* file watcher root directory deletion ("move to recycle bin") does not spawn any sane events (WatchService specifics),
  but "real" deletion works fine