package ru.gamehub;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;

import java.util.List;

public final class ApiModels {
    public static final String ID = "[A-Za-z0-9_-]{1,64}";

    private ApiModels() {
    }

    public record PlayerInput(
            @NotBlank
            @Size(max = 80)
            String name,

            @NotNull
            @Min(0)
            @Max(1_000_000)
            Long level,

            @NotBlank
            @Size(max = 32)
            String region
    ) {
    }

    public record LevelChange(
            @NotNull
            @Min(-1_000_000)
            @Max(1_000_000)
            Long delta
    ) {
    }

    public record ScoreInput(
            @NotNull
            @Pattern(regexp = ID)
            String playerId,

            @NotNull
            @DecimalMin("-1000000")
            @DecimalMax("1000000")
            Double score) {
    }

    public record AchievementInput(@NotBlank @Size(max = 100) String name) {
    }

    public record BatchPlayer(
            @NotNull
            @Pattern(regexp = ID)
            String id,

            @NotNull
            @Valid
            PlayerInput profile) {
    }

    public record BatchInput(
            @NotEmpty
            @Size(max = 1000)
            List<@NotNull @Valid BatchPlayer> players) {
    }

    public record Player(
            String id,
            String name,
            long level,
            String region,
            @JsonProperty("created_at")
            String createdAt) {
    }

    public record CachedPlayer(Player player, boolean hit) {
    }

    public record LoginResult(String playerId, long count, long ttl) {
    }

    public record RankedPlayer(String playerId, double score, long position) {
    }

    public record Ranking(String playerId, long rank, long position) {
    }

    public record BatchResult(int count, double elapsedMs) {
    }

    public record ApiError(String code, String message) {
    }
}
