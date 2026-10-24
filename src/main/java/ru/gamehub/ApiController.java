package ru.gamehub;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

import static ru.gamehub.ApiModels.*;

@Validated
@RestController
@RequestMapping("/api")
public class ApiController {
    private final GameService service;
    private final RedisAccess redis;

    public ApiController(GameService service, RedisAccess redis) {
        this.service = service;
        this.redis = redis;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "redis", redis.health());
    }

    @PostMapping("/players/{id}")
    public Player save(@PathVariable @Pattern(regexp = ID) String id, @Valid @RequestBody PlayerInput input) {
        return service.save(id, input);
    }

    @GetMapping("/players/{id}")
    public ResponseEntity<Player> get(@PathVariable @Pattern(regexp = ID) String id) {
        CachedPlayer result = service.get(id);
        return ResponseEntity.ok().header("X-Cache", result.hit() ? "HIT" : "MISS").body(result.player());
    }

    @PatchMapping("/players/{id}/level")
    public Player level(@PathVariable @Pattern(regexp = ID) String id, @Valid @RequestBody LevelChange input) {
        return service.changeLevel(id, input.delta());
    }

    @PostMapping("/players/{id}/login")
    public LoginResult login(@PathVariable @Pattern(regexp = ID) String id) {
        return service.login(id);
    }

    @PostMapping("/leaderboard/score")
    public Map<String, Object> score(@Valid @RequestBody ScoreInput input) {
        return Map.of("playerId", input.playerId(), "score", service.addScore(input));
    }

    @GetMapping("/leaderboard/top")
    public List<RankedPlayer> top(@RequestParam(defaultValue = "10") @Min(1) @Max(100) int limit,
                                  @RequestParam(defaultValue = "false") boolean consistent) {
        return service.top(limit, consistent);
    }

    @GetMapping("/leaderboard/rank/{playerId}")
    public Ranking rank(@PathVariable @Pattern(regexp = ID) String playerId,
                        @RequestParam(defaultValue = "false") boolean consistent) {
        return service.rank(playerId, consistent);
    }

    @PostMapping("/players/{id}/achievements")
    public Map<String, Boolean> achievement(@PathVariable @Pattern(regexp = ID) String id,
                                            @Valid @RequestBody AchievementInput input) {
        return Map.of("added", service.addAchievement(id, input.name()));
    }

    @GetMapping("/players/{id}/achievements/{name}")
    public Map<String, Boolean> hasAchievement(@PathVariable @Pattern(regexp = ID) String id,
                                               @PathVariable @NotBlank @Size(max = 100) String name,
                                               @RequestParam(defaultValue = "false") boolean consistent) {
        return Map.of("exists", service.hasAchievement(id, name, consistent));
    }

    @GetMapping("/players/{id1}/achievements/common/{id2}")
    public List<String> common(@PathVariable @Pattern(regexp = ID) String id1,
                               @PathVariable @Pattern(regexp = ID) String id2,
                               @RequestParam(defaultValue = "false") boolean consistent) {
        return service.common(id1, id2, consistent);
    }

    @PostMapping("/players/batch")
    public BatchResult batch(@Valid @RequestBody BatchInput input) {
        return service.batch(input);
    }
}
