
.proj.instance <- new.env(parent=emptyenv())
assign("proj", NULL, envir=.proj.instance)
assign("closed", FALSE, envir=.proj.instance)

project <- function(){ structure("instance", class = "project") }

project.instance <- function(create=FALSE) {
  if(.proj.instance$closed){ stop("closed project.") }
  if(is.null(.proj.instance$proj) && create){
    open.project(project())
  }
  .proj.instance$proj
}

open.project <- function(proj=project(), name = NULL, host = "localhost", port = 6379, password = NULL) {
  tryCatch({ redisClose() }, error = function(e){})
  suppressWarnings(redisConnect(host = host, port = port, password = password))

  if(is.null(name)){
    name <- sub(" ",".",Sys.info()["user"])
    name <- paste0("tw.default.",name)
  }
  info.self <- create.info("open.project")
  proj <- .proj.instance$proj
  # initialize
  if (is.null(proj)) {
    k.env <- paste0(name, ".env")
    k.jobs <- paste0(name, ".jobs")
    k.job.id <- paste0(name, ".job.id")
    k.queue <- paste0(name, ".queue")
    k.results <- paste0(name, ".results")
    x <- list(
      name = name,
      host = host,
      port = port,
      password = password,
      k.env = k.env,
      k.jobs = k.jobs,
      k.job.id = k.job.id,
      k.queue = k.queue,
      k.results = k.results,
      jobs.size = function() {
        as.numeric(redisHLen(project.instance()$k.jobs))
      },
      queue.size = function() {
        as.numeric(redisLLen(project.instance()$k.queue))
      },
      results.size = function() {
        as.numeric(redisHLen(project.instance()$k.results))
      }
    )
    proj <- structure(x, class = "project")
  }
  print(info.self, "Openning project, name=", proj$name)

  env <- redisGet(proj$k.env)
  if (!is.null(env)) {
    # TODO make sure won't be able to overwrite data by mistake.
    tryCatch({
      proj$k.queue <- env$k.queue
      proj$k.results <- env$k.results
    }, error = function(e) {
      stop(e,"Project name is taken or project is damaged, use a diffrent name or use a clean redis server.")
    })
    assign("proj", proj, envir = .proj.instance) # re-assign
    assign("closed", FALSE, envir = .proj.instance) # re-assign
    return(invisible(TRUE))
  }

  print(info.self,"Creating new project with name ",proj$name)
  if(redisExists(proj$k.jobs)
     || redisExists(proj$k.job.id)
     || redisExists(proj$k.queue)
     || redisExists(proj$k.results) ){
    close(proj)
    stop("Project name is taken, use a diffrent name or use a clean redis server.")
  }
  assign("proj", proj, envir = .proj.instance) # re-assign
  assign("closed", FALSE, envir = .proj.instance) # re-assign
  dispatch.env(proj)
  invisible(TRUE)
}

close.project <- function(proj=project(), erase = FALSE) {
  if(.proj.instance$closed){
    warning("closed project.")
    return(invisible(TRUE))
  }
  proj <- project.instance()
  if(erase && !is.null(proj)){
    # TODO make sure won't be able to overwrite data by mistake.
    mut <- suppressWarnings(redisDelete(proj$k.queue))
    mut <- suppressWarnings(redisDelete(proj$k.env))
    mut <- suppressWarnings(redisDelete(proj$k.jobs))
    mut <- suppressWarnings(redisDelete(proj$k.job.id))
    reset.redis()
    mut <- suppressWarnings(redisDelete(proj$k.results))
  }
  assign("closed", TRUE, envir = .proj.instance) # re-assign
  tryCatch({ redisClose() }, error = function(e){})
  invisible(TRUE)
}

#' @export
print.project <- function(proj=project()) {
  proj <- project.instance()
  if (is.null(proj)) {
    print("Empty project")
    return(invisible(NULL))
  }
  cmd <- paste0(
    "name = ", proj$name,
    "\nhost = ", proj$host,
    "\nport = ", proj$port,
    "\npassword = ", proj$password,
    "\nk.env = ", proj$k.env,
    "\nk.jobs = ", proj$k.jobs,
    "\nk.job.id = ", proj$k.job.id,
    "\nk.queue = ", proj$k.queue,
    "\nk.results = ", proj$k.results,
    "\njobs.size = ", proj$jobs.size(),
    "\nqueue.size = ", proj$queue.size(),
    "\nresults.size = ", proj$results.size()
  )
  cat(cmd)
  invisible(cmd)
}

#' @export
length.project <- function(proj=project()) {
  proj <- project.instance()
  if (is.null(proj)) {
    0
  }else{
    proj$jobs.size()
  }
}

#' @export
"[[.project" <- function(proj=project(), job.id) {
  proj <- project.instance()
  if (is.null(proj)) { return(NULL) }
  if (is.null(job.id)) { return(NULL) }
  stopifnot(is.numeric(job.id))
  if (length(job.id) == 0) { return(NULL) }
  if (length(job.id) > 1) {
    warning("only first value used.")
    job.id <- job.id[1]
  }

  job <- redisHGet(proj$k.jobs, as.character(job.id))
  if (is.null(job)) { stop("Job not found.") }
  job$result <- redisHGet(proj$k.results, as.character(job.id))
  if (is.null(job$result)) { stop("Job has not finished yet.") }
  job
}


#' @export
"[.project" <- function(proj=project(),job.id) {
  stop("This function is not available for performance concern.
       Use project()[[job.id]] to read one result or read(project()) to read all results.")
}

read.project <- function(proj=project(), blocking = TRUE, func = NULL) {
  proj <- project.instance()
  if (is.null(proj)) { return(invisible(NULL)) }
  info.self <- create.info("read.project")
  for (i in 1:10) {
    if (!blocking ||  proj$results.size() >= proj$jobs.size()) { break }
    if (proj$queue.size() > 0) {
      join.project(proj, autoexit = TRUE)
      print(info.self,"Wating for other clients...")
      for (i in 1:(60*i)) {
        if (proj$results.size() < proj$jobs.size()){
          # TODO
          #Sys.sleep(1) # give other clients a changce to finish their jobs.
        }else{
          break
        }
      }
    } else{
      repair.project(proj)
    }
  }
  if(proj$results.size() < proj$jobs.size()){ stop("Something probably went wrong.") }

  keys <- redisHKeys(proj$k.jobs)
  results <- NULL
  if(is.null(func)){
    results <- vector(mode = "list", length = length(keys))
    i <- 1
  }
  for (job.id in keys) {
    result <- redisHGet(proj$k.results, job.id)
    if (is.null(result$result)) { warning("null result") }
    if (is.null(func)) {
      results[i] <- list(result)
      i <- i + 1
    } else{
      func(result)
    }
  }
  print(info.self,"Done!")
  if(is.null(func)){
    results
  }else{
    invisible(NULL)
  }
}

repair.project <- function(proj=project(), clean = FALSE) {

  proj <- project.instance()
  if (is.null(proj)) {  return(invisible(TRUE)) }
  info.self <- create.info("repair.project")
  print(info.self,"Repair project ",proj$name)
  if (clean) {
    k.queue <- proj$k.queue
    k.results <- proj$k.results
    while (k.queue == proj$k.queue || k.results == proj$k.results) {
      suffix <- substring(sha1(runif(1))[[1]], 0, 4) # make an unique identifier
      k.queue <- paste0(proj$name, ".queue.", suffix)
      k.results <- paste0(proj$name, ".results.", suffix)
    }
    env <- redisGet(proj$k.env)
    if(is.null(env)) {
      env = list("k.queue"=k.queue,"k.results"=k.results,"env.list"=list())
    }else{
      env$k.queue = k.queue
      env$k.results = k.results
    }
    redisSet(proj$k.env,env)
    .proj.instance$proj$k.queue = k.queue
    .proj.instance$proj$k.results = k.results
    mut <- suppressWarnings(redisDelete(proj$k.queue))
    mut <- suppressWarnings(redisDelete(proj$k.results))
    reset.redis()
    mut <- suppressWarnings(redisDelete(proj$k.queue)) # reduce concurrency problem
    mut <- suppressWarnings(redisDelete(proj$k.results)) # reduce concurrency problem
  }

  # repair job queue
  if (proj$jobs.size() == 0) { return(invisible(TRUE)) }
  if (proj$results.size() >= proj$jobs.size()) { return(invisible(TRUE)) }

  jobs <- redisHKeys(proj$k.jobs)
  if (proj$results.size() == 0) {
    jobs <- as.character(jobs)
  } else{
    results <- redisHKeys(proj$k.results)
    jobs <- data.frame(matrix(
      nrow = length(jobs) ,
      ncol = 1 ,
      dimnames = list(jobs, "v")
    ))
    jobs[, 1] <- 0
    jobs[as.character(results), 1] <- 1
    jobs <- row.names(jobs)[jobs$v == 0]
  }
  for (job in jobs) {
    redisLPush(proj$k.queue, job)
  }
  return(invisible(TRUE))
}


join.project.do.work <- function(proj, arg.list, tryout = FALSE, autoexit=FALSE) {
  info.self <- create.info("join.project")
  if(is.null(arg.list)) { stop("Broken project, see repair.project() for details.") }
  env.names <- names(arg.list)
  for (env.name in env.names) {
    eval(parse(text = paste0(env.name, " <- arg.list[[env.name]]")))
  }

  do.work <- function(job) {
    if (is.null(job)) { return(0) }
    job <- redisHGet(proj$k.jobs, as.character(job))
    if (is.null(job)) { return(0) }
    print(info.self,"Processing job", job$job.id)
    if (!is.function(job$method) && !is.character(job$method)) {
      print(info.self,"Missing method to call.")
      return(0)
    }
    result <- do.call(job$method, job$arg.list)
    mut <- redisHSet(proj$k.results, as.character(job$job.id), list(job.id = job$job.id, result = result), NX = FALSE) # NX = FALSE overwrite
    if(tryout) {
      print(info.self,"result for job",job$job.id,"-->\n",result)
    }
    return(1)
  }

  repeat{
    job <- redisLPop(proj$k.queue)
    do.work(job)
    if(tryout) { return(invisible(TRUE)) }
    if(is.null(job) && autoexit ){ return(invisible(TRUE)) }
    if (is.null(job)) {
      print(info.self,"idel...")
      Sys.sleep(60)
    }
  }


}

join.project <- function(proj=project(), tryout = FALSE, autoexit=FALSE) {
  proj <- project.instance()
  info.self <- create.info("join.project")
  if (is.null(proj)) {
    warning("Empty project.")
    return(invisible(TRUE))
  }
  print(info.self,"Join project",proj$name)

  repeat {
    mut <- tryCatch({
      env <- redisGet(proj$k.env)
      if(is.null(env)){
        print(info.self, "Finished, thanks for help.")
        print("If you are not expecting this, then the project probably has been damaged, see repair.project() for details.")
        return(invisible(TRUE))
      }
      proj$k.queue <- env$k.queue
      proj$k.results <- env$k.results
      join.project.do.work(proj, env$arg.list, tryout = tryout, autoexit = autoexit)
      if(tryout || autoexit) { return(invisible(TRUE)) }
    }, error = function(e) { # never stop
      print(info.self,e)
      if(tryout || autoexit) { return(invisible(TRUE)) }
      Sys.sleep(60)
    })
  }
}

join.script <- function(proj=project(),host = NULL) {
  proj <- project.instance()
  cmd <- paste0("Rscript -e \"library(teamwork)")
  cmd <- paste0(cmd,"; open(project(),",
                "name = '",proj$name, "'," ,
                "host = '",proj$host, "'," ,
                "port =  ",proj$port)
  if(!is.null(proj$password)){
    cmd <- paste0(cmd,",password=",proj$password)
  }
  cmd <- paste0(cmd,")")
  cmd <- paste0(cmd,"; join.project()\"")
  cat(cmd)
  invisible(NULL)
}
