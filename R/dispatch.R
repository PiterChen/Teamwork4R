
dispatch <- function(method, ...) {
  proj <- project.instance(create = TRUE)
  method <- as.character(substitute(method)) # don't transfer function body,
  job <- list(method = method, arg.list = list(...))
  job$job.id <- as.numeric(redisIncr(proj$k.job.id)[[1]])
  ret <- redisHSet(proj$k.jobs, as.character(job$job.id), job, NX = TRUE) # NX = TRUE , no overwrite
  if (!as.numeric(ret[[1]]) ) {
    stop("job.id '",job$job.id,"' exists, probably means project has been damaged.")
  }
  redisLPush(proj$k.queue, job$job.id)  # push to job queue
  return(job$job.id)
}


dispatch.env <- function(...) {
  proj <- project.instance(create = TRUE)
  env <- list(arg.list=list(...))
  env$signature <- sha1(as.character(list(...))) # make a identifier for env
  env$signature <- substring(env$signature,0,8)
  env$k.queue <- proj$k.queue
  env$k.results <- proj$k.results

  info.self <- create.info("dispatch.env")
  print(info.self, "env signature ", env$signature)

  mut <- redisSet(proj$k.env, env, NX = FALSE) # NX = FALSE , overwrite force
  reset.redis()
  if(proj$jobs.size() > 0){
    warning("Project is not empty. See repair.project() for details.")
  }
  return(invisible(TRUE))
}


