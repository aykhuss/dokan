# Notes:
# `COMPREPLY`: what will be rendered after completion is triggered
# `completing_word`: currently typed word to generate completions for
# `${!var}`: evaluates the content of `var` and expand its content as a variable
#     hello="world"
#     x="hello"
#     ${!x} -> ${hello} -> "world"
_nnlojet_run() {
  _nnlojet_run_subparsers=('init' 'config' 'submit' 'finalize')

  _nnlojet_run_option_strings=('-h' '--help' '--exe' '-v' '--version')
  _nnlojet_run_init_option_strings=('-h' '--help' '-o' '--output' '--no-lumi')
  _nnlojet_run_config_option_strings=('-h' '--help' '--merge' '--advanced')
  _nnlojet_run_submit_option_strings=('-h' '--help' '--policy' '--order' '--target-rel-acc' '--job-max-runtime' '--jobs-max-total' '--jobs-max-concurrent' '--seed-offset')
  _nnlojet_run_finalize_option_strings=('-h' '--help' '--trim-threshold' '--trim-max-fraction' '--k-scan-nsteps' '--k-scan-maxdev-steps')

  _nnlojet_run_init_pos_0_COMPGEN=_compgen_files
  _nnlojet_run_init__o_COMPGEN=_compgen_dirs
  _nnlojet_run_init___output_COMPGEN=_compgen_dirs
  _nnlojet_run_config_pos_0_COMPGEN=_compgen_dirs
  _nnlojet_run_submit_pos_0_COMPGEN=_compgen_dirs
  _nnlojet_run_finalize_pos_0_COMPGEN=_compgen_dirs

  _nnlojet_run_pos_0_choices=('init' 'config' 'submit' 'finalize')
  _nnlojet_run_submit___policy_choices=('local' 'htcondor' 'slurm')
  _nnlojet_run_submit___order_choices=('lo' 'nlo' 'nlo_only' 'nnlo' 'nnlo_only')

  _nnlojet_run_pos_0_nargs=A...
  _nnlojet_run__h_nargs=0
  _nnlojet_run___help_nargs=0
  _nnlojet_run__v_nargs=0
  _nnlojet_run___version_nargs=0
  _nnlojet_run_init__h_nargs=0
  _nnlojet_run_init___help_nargs=0
  _nnlojet_run_init___no_lumi_nargs=0
  _nnlojet_run_config__h_nargs=0
  _nnlojet_run_config___help_nargs=0
  _nnlojet_run_config___merge_nargs=0
  _nnlojet_run_config___advanced_nargs=0
  _nnlojet_run_submit__h_nargs=0
  _nnlojet_run_submit___help_nargs=0
  _nnlojet_run_finalize__h_nargs=0
  _nnlojet_run_finalize___help_nargs=0

  # $1=COMP_WORDS[1]
  _compgen_files() {
    # Show only files with extension .run
    compgen -f -- $1 | while read f ; do [ -d "$f" -o "$f" = *.run ] && echo $f ; done  # files
  }

  # $1=COMP_WORDS[1]
  _compgen_dirs() {
    compgen -d -- $1  # recurse into subdirs
  }

  # $1=COMP_WORDS[1]
  _replace_nonword() {
    echo "${1//[^[:word:]]/_}"
  }

  # set default values (called for the initial parser & any subparsers)
  _set_parser_defaults() {
    local subparsers_var="${prefix}_subparsers[@]"
    sub_parsers=${!subparsers_var-}

    local current_option_strings_var="${prefix}_option_strings[@]"
    current_option_strings=${!current_option_strings_var}

    completed_positional_actions=0

    _set_new_action "pos_${completed_positional_actions}" true
  }

  # $1=action identifier
  # $2=positional action (bool)
  # set all identifiers for an action's parameters
  _set_new_action() {
    current_action="${prefix}_$(_replace_nonword $1)"

    local current_action_compgen_var=${current_action}_COMPGEN
    current_action_compgen="${!current_action_compgen_var-}"

    local current_action_choices_var="${current_action}_choices[@]"
    current_action_choices="${!current_action_choices_var-}"

    local current_action_nargs_var="${current_action}_nargs"
    if [ -n "${!current_action_nargs_var-}" ]; then
      current_action_nargs="${!current_action_nargs_var}"
    else
      current_action_nargs=1
    fi

    current_action_args_start_index=$(( $word_index + 1 - $pos_only ))

    current_action_is_positional=$2
  }


  local completing_word="${COMP_WORDS[COMP_CWORD]}"
  local completed_positional_actions
  local current_action
  local current_action_args_start_index
  local current_action_choices
  local current_action_compgen
  local current_action_is_positional
  local current_action_nargs
  local current_option_strings
  local sub_parsers
  COMPREPLY=()

  local prefix=_nnlojet_run
  local word_index=0
  local pos_only=0 # "--" delimeter not encountered yet
  _set_parser_defaults
  word_index=1

  # determine what arguments are appropriate for the current state
  # of the arg parser
  while [ $word_index -ne $COMP_CWORD ]; do
    local this_word="${COMP_WORDS[$word_index]}"

    if [[ $pos_only = 1 || " $this_word " != " -- " ]]; then
      if [[ -n $sub_parsers && " ${sub_parsers[@]} " == *" ${this_word} "* ]]; then
        # valid subcommand: add it to the prefix & reset the current action
        prefix="${prefix}_$(_replace_nonword $this_word)"
        _set_parser_defaults
      fi

      if [[ " ${current_option_strings[@]} " == *" ${this_word} "* ]]; then
        # a new action should be acquired (due to recognised option string or
        # no more input expected from current action);
        # the next positional action can fill in here
        _set_new_action $this_word false
      fi

      if [[ "$current_action_nargs" != "*" ]] && \
         [[ "$current_action_nargs" != "+" ]] && \
         [[ "$current_action_nargs" != *"..." ]] && \
         (( $word_index + 1 - $current_action_args_start_index - $pos_only >= \
            $current_action_nargs )); then
        $current_action_is_positional && let "completed_positional_actions += 1"
        _set_new_action "pos_${completed_positional_actions}" true
      fi
    else
      pos_only=1 # "--" delimeter encountered
    fi

    let "word_index+=1"
  done

  # Generate the completions

  if [[ $pos_only = 0 && "${completing_word}" == -* ]]; then
    # optional argument started: use option strings
    COMPREPLY=( $(compgen -W "${current_option_strings[*]}" -- "${completing_word}") )
  else
    # use choices & compgen
    local IFS=$'\n' # items may contain spaces, so delimit using newline
    COMPREPLY=( $([ -n "${current_action_compgen}" ] \
                  && "${current_action_compgen}" "${completing_word}") )
    unset IFS
    COMPREPLY+=( $(compgen -W "${current_action_choices[*]}" -- "${completing_word}") )
  fi

  return 0
}

complete -o filenames -F _nnlojet_run nnlojet-run
