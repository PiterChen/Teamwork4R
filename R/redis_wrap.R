
#kill all other clients' connection
reset.redis <- function(){
  info.self <- create.info("redis.reset")
  mut <- redisCmd("CLIENT", "KILL","TYPE","normal")
  #TODO remove comment Sys.sleep(5) # reduce concurrency problem
  clients <- strsplit(clients <- redisCmd("CLIENT","LIST")[[1]],"\n")[[1]]
  for(a in clients){
    client <- as.list(strsplit(sub(".+?=", "", a) , " +.+?=")[[1]])
    names(client) <- strsplit(paste0(a, " "), "=.+? +")[[1]]
    print(info.self, "remained client", client$id, "addr=", client$addr)
  }
  invisible(NULL)
}

# reconnect to redis server
reconnect.redis <- function() {
  mut <- suppressWarnings(tryCatch({
    mut <- redisCmd("PING")
  },
  error = function(e) {
    redisCmd("PING")
  }))
  invisible(NULL)
}





