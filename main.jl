# load in required packages
using CSV
using DataFrames
using Tidier
using JLD2 # conserves RAM by saving data to file

# structure for holding the results for each permutation
struct Possibility 
    case::Vector{Bool}
    probability::Float64

    # team_results::DataFrame
    team_index::Vector{Int} ## storing the index is probably not necessary
    wins::Vector{Float64}
    result::Vector{String}
end

# generates each permutation
function boolean_permutations(n::Int)::Vector{Vector{Bool}}
    vec = fill(false, n)
    permutations = Vector{Vector{Bool}}()

    function generate_permutations(curr_index)
        if curr_index == n
            push!(permutations, copy(vec))
            return
        end

        for value in [false, true]
            vec[curr_index + 1] = value
            generate_permutations(curr_index + 1)
        end
    end

    generate_permutations(0)
    return permutations
end

function evaluate_permutation(end_of_season_ladder::DataFrame)::DataFrame
    sorted_wins = sort(end_of_season_ladder.wins, rev = true)
    eighth_place_wins = sorted_wins[8]
    ninth_place_wins = sorted_wins[9]


    result = end_of_season_ladder.wins .|> ((wins) -> begin
            if (wins == eighth_place_wins) && (wins == ninth_place_wins)
                return "indefinite"
            elseif wins >= eighth_place_wins
                return "make"
            else return "miss"
            end
        end
    )

    return DataFrame(team_index = 1:length(teams), wins = end_of_season_ladder.wins, result = result)

end

# return dataframe with cols team, result = enum{'make', 'miss', 'indefinite'}
function run_permutation(permutation::Vector{Bool})::Possibility
    # find the probability of this specific outcome occurring
    probability = 1:length(permutation) .|> 
        (index -> permutation[index] ? remaining_matches.agg_probs[index] : 1 - remaining_matches.agg_probs[index]) |>   
        prod

    result_ladder = copy(ladder)

    for index in 1:length(permutation) 
        if permutation[index]
            result_ladder.wins[findfirst(x -> x == remaining_matches.hteam[index], teams)] += 1
        else 
            result_ladder.wins[findfirst(x -> x == remaining_matches.ateam[index], teams)] += 1
        end
    end

    permutation_evaluation = evaluate_permutation(result_ladder)

    return Possibility(
        permutation, probability, 
        permutation_evaluation.team_index, permutation_evaluation.wins, permutation_evaluation.result
    )
end

# PATH TO PREDICTIONS FILE: CHANGE THIS YOURSELF (required cols are hteam ateam hscore ascore agg_probs)
file_path = "predictions.csv"

file = CSV.File(file_path, missingstring = "NA")
nrl_data = file |> DataFrame

teams = (@chain nrl_data begin
    @pull(hteam)
end) |> unique |> sort

no_wins = teams .|> ((team) -> begin
    global global_team = team # because for some reason, you can't reference stuff nicely

    # get number of wins
    wins = (@chain nrl_data begin 
        @filter(year == 2023 && (hteam == @eval(Main, global_team) || ateam == @eval(Main, global_team)))
        @filter((hteam == @eval(Main, global_team) && hscore > ascore) | (ateam == @eval(Main, global_team) && ascore > hscore))
        @summarize(wins = n())
        @pull(wins)
    end)[1]

    # add on number of draws / 2
    wins += (@chain nrl_data begin 
        @filter(year == 2023 && (hteam == @eval(Main, global_team) || ateam == @eval(Main, global_team)) && hscore == ascore)
        @summarize(draws = n())
        @pull(draws)
    end)[1] / 2
end 
)

ladder = DataFrame(
    team = teams, 
    wins = no_wins
)

println(@chain ladder begin @arrange(desc(wins)) end)

remaining_matches = @chain nrl_data begin 
    @filter(ismissing(hscore))
end

# remaining_matches = last(nrl_data, 24)

MATCHES = nrow(remaining_matches)
SAVE_FREQUENCY = 250_000 # change this based on how large you'd like each file to be
perms = boolean_permutations(MATCHES)


##### DO CHECKS; COMMENT OUT IF YOU DON'T NEED THEM #####


# init values
result_vec = Vector{Possibility}()
iterations = 0

println()
println("Beginning checks now.")

for perm in perms
    push!(result_vec, run_permutation(perm))

    global iterations += 1
    
    if iterations % SAVE_FREQUENCY == 0 || iterations == length(perms)
        println("iteration $iterations complete")

        JLD2.save_object("output$(ceil(Int, iterations / SAVE_FREQUENCY))", result_vec)
        empty!(result_vec)
        global result_vec = Vector{Possibility}()
    end
end

println("All done!")



##### END CHECKS #####


### ANALYSIS HERE ###

teams .|> 
    (team -> begin
        println()

        println("Analysing \"$(team)\"")

        team_index = findfirst(x -> x == team, ladder.team)
        result_df = DataFrame(result = ["make", "indefinite", "miss"], probability = [0.0,0.0,0.0], count = [0,0,0], average_wins = [0.0,0.0,0.0])
        
        # could make this into a struct?
        sum_make_wins = 0.0;
        sum_indefinite_wins = 0.0;
        sum_miss_wins = 0.0;

        make_results = fill(0.0, MATCHES)
        indefinite_results = fill(0.0, MATCHES)
        miss_results = fill(0.0, MATCHES)

        for file_index in 1:(ceil(Int, length(perms) / SAVE_FREQUENCY))
            result_vec = JLD2.load_object("output$(file_index)")

            print("\r==> Interpreting file $(file_index)/$(ceil(Int, length(perms) / SAVE_FREQUENCY))")

            for result::Possibility in result_vec 
                case = result.case
                team_result = result.result[team_index]
                team_wins = result.wins[team_index]
                
    
                if team_result == "make"
                    result_df.probability[1] += result.probability
                    result_df.count[1] += 1
                    sum_make_wins += team_wins * result.probability 
                    make_results .+= case .* result.probability # is this right?
                elseif team_result == "indefinite"
                    result_df.probability[2] += result.probability
                    result_df.count[2] += 1
                    sum_indefinite_wins += team_wins * result.probability
                    indefinite_results .+= case .* result.probability
                else
                    result_df.probability[3] += result.probability
                    result_df.count[3] += 1
                    sum_miss_wins += team_wins * result.probability
                    miss_results .+= case .* result.probability
                end
            end
        end

        result_df.average_wins = [sum_make_wins, sum_indefinite_wins, sum_miss_wins] ./ result_df.probability
    
        normalised_results = Dict(
            "make" => make_results ./ result_df.probability[1], 
            "indefinite" => indefinite_results ./ result_df.probability[2],
            "miss" => miss_results  ./ result_df.probability[3]
        )

        println()
        println(result_df)
        println()

        for result in keys(normalised_results)
            if sum(normalised_results[result]) == 0
                continue
            end

            println("When the $team achieve result '$result':")

            ordered_indices = 1:length(normalised_results[result])
            
            for match_index in ordered_indices
                hteam = remaining_matches.hteam[match_index]
                ateam = remaining_matches.ateam[match_index]

                hteam_required_win_more_often = normalised_results[result][match_index] > 0.5 
                team_win_rate = hteam_required_win_more_often ? normalised_results[result][match_index] : 1 - normalised_results[result][match_index]
                actual_probability = hteam_required_win_more_often ? remaining_matches.agg_probs[match_index] : 1 - remaining_matches.agg_probs[match_index]

                probability_difference = round(team_win_rate - actual_probability, digits = 3)

                if (probability_difference == 0)
                    print("  0.0%")
                else 
                    printstyled("$(probability_difference < 0.1 ? " " : "")$(probability_difference > 0 ? "+" : "")$(round(100 * probability_difference, digits = 1))%", color = probability_difference > 0 ? :blue : :red)
                end
                println("   $(hteam_required_win_more_often ? string(hteam, " wins v ", ateam) : string(ateam, " wins @ ", hteam)) $(round(team_win_rate * 100, digits=1))% of the time [$(round(actual_probability * 100, digits = 1))%]")

                if match_index % 8 == 0
                    println()
                end

            end
            println()
        end

        println()
    end
)

### END ANALYSIS












