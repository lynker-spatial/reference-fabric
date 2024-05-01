#' Track files in a directory
#' @param name Name of target
#' @param directory Path to directory to list
#' @param pattern Optional pattern for filtering directory
#' @inherit tarchetypes::tar_files_input
tar_list_files <- function(
    name,
    directory,
    pattern = NULL,
    format = c("file", "file_fast", "url", "aws_file"),
    repository = targets::tar_option_get("repository"),
    iteration = targets::tar_option_get("iteration"),
    error = targets::tar_option_get("error"),
    memory = targets::tar_option_get("memory"),
    garbage_collection = targets::tar_option_get("garbage_collection"),
    priority = targets::tar_option_get("priority"),
    resources = targets::tar_option_get("resources"),
    cue = targets::tar_option_get("cue")
) {
    tarchetypes::tar_files_input(
        substitute(name),
        list.files(directory, pattern = pattern, full.names = TRUE),
        format = format,
        repository = repository,
        iteration = iteration,
        error = error,
        memory = memory,
        garbage_collection = garbage_collection,
        priority = priority,
        resources = resources,
        cue = cue
    )
}

tar_format_path <- targets::tar_format(
    read  = function(path) readLines(path),
    write = function(object, path) writeLines(object, path)
)

tar_dir_path <- function(name, path, ...) {
    targets::tar_target_raw(
        as.character(substitute(name)),
        substitute({
            dir <- file.path(path, ...)
            if (!dir.exists(dir)) {
                dir.create(dir, showWarnings = FALSE, recursive = TRUE)
            }
            dir
        }),
        format = tar_format_path
    )
}

tar_file_path <- function(name, path, ..., pattern = NULL) {
    targets::tar_target_raw(
        as.character(substitute(name)),
        substitute(file.path(path, ...)),
        format = tar_format_path,
        pattern = substitute(pattern)
    )
}
