package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	"websocket/internal/brain"
	"websocket/internal/media"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := media.DefaultConfig()
	denoiseCfg := media.DenoiseConfigFromEnv()
	asrCfg := media.ASRConfigFromEnv()
	amdCfg := media.AMDConfigFromEnv()
	endpointCfg := media.EndpointConfigFromEnv()
	semanticCfg := media.SemanticTurnConfigFromEnv()
	backchannelCfg := media.BackchannelConfigFromEnv()
	brainCfg := brain.ConfigFromEnv()
	ttsCfg := media.TTSConfigFromEnv()
	egressCfg := media.EgressConfigFromEnv()
	bargeInCfg := media.BargeInConfigFromEnv()
	localVADEnabled, localVADSilero := media.LocalVADConfigFromEnv()
	carrierCfg := media.CarrierConfigFromEnv()
	carrierProfile := carrierCfg.Profile()
	voicemailCfg := media.VoicemailConfigFromEnv()
	carrierSerializer := media.NewCarrierSerializer(carrierCfg)
	sessionCloser := &media.SessionCloserHolder{}
	cfg.Carrier = carrierCfg

	if addr := os.Getenv("LISTEN_ADDR"); addr != "" {
		cfg.ListenAddr = addr
	}
	if rate := os.Getenv("TARGET_SAMPLE_RATE"); rate != "" {
		if v, err := strconv.Atoi(rate); err == nil {
			cfg.TargetSampleRate = v
		}
	} else if carrierProfile.Variant == media.CarrierAsterisk {
		cfg.TargetSampleRate = carrierProfile.InputSampleRate
	}
	if ms := os.Getenv("FRAME_DURATION_MS"); ms != "" {
		if v, err := strconv.Atoi(ms); err == nil {
			cfg.FrameDurationMs = v
		}
	}
	if carrierProfile.Variant == media.CarrierAsterisk && os.Getenv("TTS_OUTPUT_FORMAT") == "" {
		ttsCfg.OutputFormat = "pcm_24000"
	}
	sessionCloser.SetCarrierProfile(carrierProfile)

	denoiser, err := media.NewDenoiser(denoiseCfg)
	if err != nil {
		logger.Error("denoiser init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := denoiser.Close(); err != nil {
			logger.Warn("denoiser close failed", "error", err)
		}
	}()

	asrProvider, err := media.NewASRProvider(asrCfg)
	if err != nil {
		logger.Error("asr provider init failed", "error", err)
		os.Exit(1)
	}

	amdClassifier, err := media.NewAMDClassifier(amdCfg)
	if err != nil {
		logger.Error("amd classifier init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := amdClassifier.Close(); err != nil {
			logger.Warn("amd classifier close failed", "error", err)
		}
	}()

	localVAD := media.NewLocalVAD(localVADEnabled, localVADSilero)
	turnListener := media.NewLoggingTurnListener(logger)

	semanticTurn, err := media.NewSemanticTurnDetector(semanticCfg)
	if err != nil {
		logger.Error("semantic turn detector init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := semanticTurn.Close(); err != nil {
			logger.Warn("semantic turn detector close failed", "error", err)
		}
	}()

	backchannel, err := media.NewBackchannelClassifier(backchannelCfg)
	if err != nil {
		logger.Error("backchannel classifier init failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := backchannel.Close(); err != nil {
			logger.Warn("backchannel classifier close failed", "error", err)
		}
	}()

	ttsProvider, err := media.NewTTSProvider(ttsCfg)
	if err != nil {
		logger.Error("tts provider init failed", "error", err)
		os.Exit(1)
	}

	target := cfg.TargetFormat()
	metricsCfg := media.MetricsConfigFromEnv()
	metrics := media.NewMetrics(metricsCfg)
	watchdogCfg := media.WatchdogConfigFromEnv()
	latencyBudget := media.LatencyBudgetFromEnv()

	sinkFactory := func() media.AudioSink {
		sessionClock := media.RealClock{}
		callControl := brain.NewCallControl(brain.CallControlConfig{
			AMDEnabled: amdCfg.Enabled,
			Voicemail:  voicemailCfg,
			Logger:     logger,
		})

		turnManager := media.NewTurnManager(
			turnListener,
			endpointCfg,
			sessionClock,
			localVAD,
			semanticTurn,
			semanticCfg,
			backchannel,
			logger,
		)

		var replyConsumer media.ReplyConsumer = media.NewLoggingReplyConsumer(logger)
		var ttsConsumer *media.TTSReplyConsumer
		var carrierEgress *media.CarrierEgress
		var obs *media.SessionObservability
		var brainClient *brain.Client

		if ttsCfg.Enabled {
			stream, err := ttsProvider.Open(context.Background(), media.TTSSessionMeta{})
			if err != nil {
				logger.Warn("tts stream open failed; using logging reply consumer", "error", err)
			} else {
				carrierEgress = media.NewCarrierEgress(egressCfg, cfg.FrameDurationMs, sessionClock, carrierSerializer, carrierProfile, logger)
				onEndCall := func(ctx context.Context, s *media.Session) {
					sessionCloser.EndCallSession(ctx, s)
				}
				ttsConsumer = media.NewTTSReplyConsumer(stream, carrierEgress, turnManager, onEndCall, logger)
				replyConsumer = ttsConsumer
			}
		}

		if brainCfg.Enabled {
			brainClient = brain.NewClient(brainCfg, replyConsumer, turnManager, logger)
			turnManager.SetListener(brainClient)
		}

		callControl.Bind(brainClient, ttsConsumer, carrierEgress, sessionCloser)
		amdListener := media.NewMetricsAMDListener(callControl, metrics)

		if ttsConsumer != nil || brainClient != nil || carrierEgress != nil {
			obs = &media.SessionObservability{Metrics: metrics}
		}

		if obs != nil {
			obs.Timing = media.NewTurnTimingHub("", sessionClock, logger, metrics, latencyBudget)
			obs.TurnManager = turnManager
			if ttsConsumer != nil {
				obs.Watchdog = media.NewDeadAirWatchdog(watchdogCfg, sessionClock, ttsConsumer, metrics, logger)
				ttsConsumer.SetObservability(obs.Timing, obs.Watchdog)
			}
			turnManager.SetObservability(obs.Timing, obs.Watchdog)
			if carrierEgress != nil {
				carrierEgress.SetObservability(obs.Timing, obs.Watchdog)
			}
			if brainClient != nil {
				brainClient.SetObservability(obs.Timing, obs.Watchdog)
			}
		}

		if bargeInCfg.Enabled && carrierEgress != nil && ttsConsumer != nil {
			bargeIn := media.NewBargeInHandler(
				bargeInCfg,
				carrierEgress,
				ttsConsumer,
				brainClient,
				turnManager,
				sessionClock,
				logger,
			)
			bargeIn.SetObservability(obs.Timing, obs.Watchdog, metrics)
			turnManager.SetBargeInHandler(bargeIn)
		}

		pipeline := media.NewTranscodeSink(
			media.NewDenoiseSink(
				media.NewAMDGateSink(
					media.NewASRSink(
						asrProvider,
						turnManager,
						target.SampleRate,
						logger,
					),
					amdClassifier,
					amdListener,
					target.SampleRate,
					amdCfg,
					logger,
				),
				denoiser,
				media.NoopAEC{},
				target.SampleRate,
				logger,
			),
			target,
			cfg.FrameDurationMs,
			logger,
		)

		if brainClient != nil || ttsConsumer != nil {
			return &brain.BootstrapSink{
				Inner: pipeline, Brain: brainClient, TTSReply: ttsConsumer,
				CarrierEgress: carrierEgress, Observability: obs,
				AMDEnabled: amdCfg.Enabled, CallControl: callControl,
			}
		}
		return pipeline
	}

	logger.Info("audio pipeline ready",
		"target_sample_rate", target.SampleRate,
		"frame_duration_ms", cfg.FrameDurationMs,
		"carrier", carrierCfg.Variant,
		"denoise_enabled", denoiseCfg.Enabled,
		"amd_enabled", amdCfg.Enabled,
		"voicemail_action", voicemailCfg.Action,
		"asr_enabled", asrCfg.Enabled,
		"local_vad_enabled", localVADEnabled,
		"semantic_turn_enabled", semanticCfg.Enabled,
		"backchannel_enabled", backchannelCfg.Enabled,
		"brain_ws_enabled", brainCfg.Enabled,
		"tts_enabled", ttsCfg.Enabled,
		"metrics_enabled", metricsCfg.Enabled,
	)

	srv := media.NewServer(cfg, logger, sinkFactory, metrics)
	sessionCloser.SetManager(srv.Manager())
	if err := srv.Run(context.Background()); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
