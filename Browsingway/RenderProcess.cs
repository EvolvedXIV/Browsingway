using Browsingway.Common;
using Browsingway.Common.Ipc;
using Dalamud.Plugin.Services;
using System.Diagnostics;

namespace Browsingway;

internal class RenderProcess : IDisposable
{
	public event EventHandler? Crashed;
	public BrowsingwayRpc? Rpc { get; private set; }

	// Forwarded from the current Rpc, which is replaced whenever the render process is restarted.
	public event Action<RendererReadyMessage>? RendererReady;
	public event Action<SetCursorMessage>? SetCursor;
	public event Action<UpdateTextureMessage>? UpdateTexture;

	/// <summary>
	/// True once the current render process has connected and reported ready. Anything sent to it before
	/// then can be lost, and nothing would send it again.
	/// </summary>
	public bool IsReady => _isReady;

	private volatile bool _isReady;

	private readonly string _configDir;
	private readonly DependencyManager _dependencyManager;

	private string _ipcChannelName = null!;

	private string _keepAliveHandleName = null!;
	private readonly int _parentPid;
	private readonly string _pluginDir;

	private DateTime _lastRenderCheck = DateTime.MinValue;
	private uint _restartCount = 0;

	private const uint _maxRestarts = 5;
	private const uint _checkDelaySeconds = 1;
	private const uint _processOkAfterSeconds = 5;

	private Process _process;
	private bool _running;

	public RenderProcess(int pid,
		string pluginDir,
		string configDir,
		DependencyManager dependencyManager,
		IPluginLog pluginLog
	)
	{
		_dependencyManager = dependencyManager;
		_pluginDir = pluginDir;
		_configDir = configDir;
		_parentPid = pid;

		CreateChannel();

		_process = SetupProcess();
	}

	private void CreateChannel()
	{
		// The names must be unique per render process, not just per game process. With the PID alone, a reloaded
		// plugin reuses the names of the previous instance while that instance's RpcBuffer (whose Dispose is
		// deferred to its read thread) or renderer may still hold them, and SharedMemory picks master/slave by
		// whoever owns the named mutex. The renderer can then join a half torn down channel and die with a
		// FileNotFoundException from MemoryMappedFile.OpenExisting. A restarted renderer gets a new channel too:
		// the old one may still hold messages meant for the dead process.
		string instanceId = Guid.NewGuid().ToString("N");
		_keepAliveHandleName = $"BrowsingwayRendererKeepAlive{_parentPid}_{instanceId}";
		_ipcChannelName = $"BrowsingwayRendererIpcChannel{_parentPid}_{instanceId}";

		BrowsingwayRpc rpc = new(_ipcChannelName);
		rpc.RendererReady += msg =>
		{
			if (msg.HasDxSharedTexturesSupport)
			{
				_isReady = true;
			}

			RendererReady?.Invoke(msg);
		};
		rpc.SetCursor += msg => SetCursor?.Invoke(msg);
		rpc.UpdateTexture += msg => UpdateTexture?.Invoke(msg);
		Rpc = rpc;
	}

	public void Dispose()
	{
		Stop();

		_process.Dispose();
		Rpc?.Dispose();
	}

	public void Start()
	{
		if (_running)
		{
			return;
		}

		_process.Start();
		_process.BeginOutputReadLine();
		_process.BeginErrorReadLine();

		_running = true;
	}

	private int _restarting = 0; // This needs to be a numeric type for Interlocked.Exchange

	public void EnsureRenderProcessIsAlive()
	{
		if (!_running)
		{
			return;
		}

		// only check every second, reduces stress on the render thread
		if (DateTime.Now - _lastRenderCheck < TimeSpan.FromSeconds(_checkDelaySeconds))
		{
			return;
		}

		_lastRenderCheck = DateTime.Now;

		if (!HasProcessExited())
		{
			// process is still running, reset restart counter if it ran for at least 5 seconds
			if (_restartCount > 0 && DateTime.Now - _process.StartTime > TimeSpan.FromSeconds(_processOkAfterSeconds))
			{
				_restartCount = 0;
			}

			return;
		}

		if (_restartCount >= _maxRestarts)
		{
			Services.PluginLog.Error("Render process is crashing in a loop - please check the logs. No further restarts will be attempted until Browsingway is restarted.");
			_isReady = false;
			Stop();
			Rpc?.Dispose();
			Rpc = null;
			OnProcessCrashed();
			return;
		}

		Task.Run(() =>
		{
			if (_hasExited && 0 == Interlocked.Exchange(ref _restarting, 1))
			{
				try
				{
					// process crashed, restart
					_restartCount++;
					string exitCode;
					try { exitCode = $"{_process.ExitCode} (0x{_process.ExitCode:X8})"; }
					catch (Exception) { exitCode = "unknown"; }

					Services.PluginLog.Error($"Render process exited with code {exitCode} - will restart asap (attempt {_restartCount}/{_maxRestarts}).");

					_isReady = false;
					Rpc?.Dispose();
					CreateChannel();
					_process = SetupProcess();
					_process.Start();
					_process.BeginOutputReadLine();
					_process.BeginErrorReadLine();

					// notify everyone that we have to reinit
					OnProcessCrashed();

					// reset the process exit flag
					_hasExited = false;
				}
				catch (Exception e)
				{
					Services.PluginLog.Error(e, "Failed to restart render process");
				}
				finally
				{
					Interlocked.Exchange(ref _restarting, 0);
				}
			}
		});
	}

	public void Stop()
	{
		if (!_running) { return; }

		_running = false;

		// Grab the handle the process is waiting on and open it up
		EventWaitHandle handle = new(false, EventResetMode.ManualReset, _keepAliveHandleName);
		handle.Set();
		handle.Dispose();

		// Give the process a sec to gracefully shut down, then kill it
		_process.WaitForExit(1000);
		try { _process.Kill(); }
		catch (InvalidOperationException) { }
	}

	private bool _hasExited = false;
	private int _checkingExited = 0; // This needs to be a numeric type for Interlocked.Exchange

	private bool HasProcessExited()
	{
		// Process.HasExited can be an expensive call (on some systems?), so it's
		// offloaded to a Task, here. This could be related to Riot's Vanguard
		// kernel anti-cheat. The performance bottleneck occurs in ntdll, so this
		// is difficult to isolate and debug.
		Task.Run(() =>
		{
			if (!_hasExited && 0 == Interlocked.Exchange(ref _checkingExited, 1))
			{
				try
				{
					_hasExited = _process.HasExited;
				}
				catch (Exception e)
				{
					Services.PluginLog.Error(e, "Failed to get process exit status");
				}
				finally
				{
					Interlocked.Exchange(ref _checkingExited, 0);
				}
			}
		});

		return _hasExited;
	}

	private Process SetupProcess()
	{
		string cefAssemblyDir = _dependencyManager.GetDependencyPathFor("cef");

		RenderParams processArgs = new()
		{
			ParentPid = _parentPid,
			DalamudAssemblyDir = Path.GetDirectoryName(typeof(IPluginLog).Assembly.Location)!,
			CefAssemblyDir = cefAssemblyDir,
			CefCacheDir = Path.Combine(_configDir, "cef-cache"),
			DxgiAdapterLuidLow = DxHandler.AdapterLuid.LowPart,
			DxgiAdapterLuidHigh = DxHandler.AdapterLuid.HighPart,
			KeepAliveHandleName = _keepAliveHandleName,
			IpcChannelName = _ipcChannelName
		};

		Process process = new();
		process.StartInfo = new ProcessStartInfo
		{
			FileName = Path.Combine(_pluginDir, "renderer", "Browsingway.Renderer.exe"),
			Arguments = RenderParamsSerializer.Serialize(processArgs),
			UseShellExecute = false,
			CreateNoWindow = true,
			RedirectStandardOutput = true,
			RedirectStandardError = true
		};

		process.OutputDataReceived += (_, args) => Services.PluginLog.Info($"[Render]: {args.Data}");
		process.ErrorDataReceived += (_, args) => Services.PluginLog.Error($"[Render]: {args.Data}");

		return process;
	}

	private void OnProcessCrashed()
	{
		Crashed?.Invoke(this, EventArgs.Empty);
	}
}