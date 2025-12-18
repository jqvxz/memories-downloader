import concurrent.futures
import json
import os
import shutil
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

import requests
from webdav3.client import Client

import tkinter as tk
from tkinter import filedialog, messagebox, ttk


# config and helper utilities

def get_desktop_dir() -> Path:
    # Return the current users Desktop path on Windows/Linux
    home = Path.home()
    desktop = home / "Desktop"
    if desktop.exists():
        return desktop
    return home


def read_memories_json(json_path: Path):
    # Load Snapchat memories JSON file (supports "Saved Media" or flat list formats)[web:14][web:27]
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "Saved Media" in data:
        memories = data["Saved Media"]
    else:
        memories = data

    if not isinstance(memories, list):
        raise ValueError("Unexpected JSON structure for memories_history.json")

    return memories


def ensure_dir(path: Path):
    # Create directory and parents if they do not exist
    path.mkdir(parents=True, exist_ok=True)


def build_output_dir(base_name: str = "Snapchat_Memories_Backup") -> Path:
    # Build a timestamped output directory on the Desktop
    desktop = get_desktop_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = desktop / f"{base_name}_{ts}"
    ensure_dir(out)
    return out


def sanitize_filename(name: str) -> str:
    # Sanitize filenames for Windows/Linux compatibility
    bad = '<>:"/\\|?*'
    for ch in bad:
        name = name.replace(ch, "_")
    return name.strip()


# multi-threaded memories downloader

class MemoryDownloader:
    def __init__(self, memories, output_root: Path, max_workers: int = 16, status_callback=None, log_callback=None):
        # Initialize downloader with memories list, output directory and worker count
        self.memories = memories
        self.output_root = output_root
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.total = len(memories)
        self.completed = 0
        self.skipped = 0
        self.failed = 0
        self.status_callback = status_callback
        self.log_callback = log_callback

    def _update_status(self, text: str):
        # Update status text via callback if available
        if self.status_callback:
            self.status_callback(text)
        else:
            print(text)

    def _log(self, text: str):
        # Send text to log callback and stdout
        if self.log_callback:
            self.log_callback(text + "\n")
        print(text)

    def _parse_date(self, memory) -> datetime:
        # Parse the "Date" field from Snapchat memories JSON, fallback to now on failure[web:27]
        raw = memory.get("Date") or memory.get("date") or ""
        raw = raw.replace(" UTC", "").strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return datetime.now()

    def _infer_extension(self, memory) -> str:
        # Infer file extension based on "Media Type" field (PHOTO/VIDEO)
        mtype = (memory.get("Media Type") or memory.get("media_type") or "").upper()
        if mtype == "VIDEO":
            return ".mp4"
        if mtype == "PHOTO":
            return ".jpg"
        return ".bin"

    def _get_download_endpoint(self, memory) -> str:
        # Get Snapchat "Download Link" endpoint from the JSON entry[web:27]
        link = memory.get("Download Link") or memory.get("download_link")
        if not link:
            raise ValueError("No 'Download Link' in memory entry")
        return link

    def _safe_request(self, session: requests.Session, method: str, url: str, **kwargs):
        # Perform a HTTP request with sane timeouts and error handling[web:186]
        kwargs.setdefault("timeout", (5, 60))
        try:
            resp = session.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            raise RuntimeError("Network timeout while contacting Snapchat servers")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Network error: {e}")

    def _download_single(self, idx: int, memory) -> Path | None:
        # Download a single memory item and save it into a dated subfolder
        try:
            date = self._parse_date(memory)
            ext = self._infer_extension(memory)
            dl_link = self._get_download_endpoint(memory)

            snap_id = "id"
            if "&mid=" in dl_link:
                snap_id = dl_link.split("&mid=", 1)[1].split("&", 1)[0]
            elif "id=" in dl_link:
                snap_id = dl_link.split("id=", 1)[1].split("&", 1)[0]

            fname = f"{date.strftime('%Y-%m-%d_%H%M%S')}_{snap_id}{ext}"
            fname = sanitize_filename(fname)

            year_dir = self.output_root / str(date.year)
            ensure_dir(year_dir)
            out_path = year_dir / fname

            if out_path.exists():
                with self.lock:
                    self.skipped += 1
                    self.completed += 1
                return out_path

            session = requests.Session()

            token_resp = self._safe_request(session, "POST", dl_link)
            real_url = token_resp.text.strip()

            media_resp = self._safe_request(session, "GET", real_url, stream=True)
            with out_path.open("wb") as f:
                shutil.copyfileobj(media_resp.raw, f)

            with self.lock:
                self.completed += 1

            return out_path

        except Exception as e:
            with self.lock:
                self.failed += 1
                self.completed += 1
            self._update_status(f"[ERROR] Memory {idx+1}/{self.total} failed: {e}")
            self._log(f"[ERROR] Memory {idx+1}/{self.total} failed: {e}")
            return None

    def download_all(self):
        # Run multi-threaded download for all memories with a simple progress display
        self._log(f"Starting download of {self.total} memories with {self.max_workers} parallel tasks...")
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._download_single, idx, mem)
                for idx, mem in enumerate(self.memories)
            ]

            while True:
                done = sum(f.done() for f in futures)
                percent = (done / self.total) * 100 if self.total else 100
                progress_text = (
                    f"Progress {done}/{self.total} ({percent:.1f}%) "
                    f"OK {self.completed - self.skipped - self.failed} "
                    f"Skip {self.skipped} Fail {self.failed}"
                )
                self._update_status(progress_text)
                if done == self.total:
                    break
                time.sleep(0.5)

        elapsed = time.time() - start
        summary = (
            f"Finished in {elapsed:.1f}s "
            f"(OK {self.completed - self.skipped - self.failed}, "
            f"Skip {self.skipped}, Fail {self.failed})"
        )
        self._update_status(summary)
        self._log(summary)


# zipping and webdav upload

def zip_folder(folder: Path, zip_path: Path, status_callback=None, log_callback=None):
    # Zip an entire folder recursively into a single archive
    text = f"Zipping {folder} -> {zip_path}"
    if status_callback:
        status_callback(text)
    if log_callback:
        log_callback(text + "\n")
    else:
        print(text)

    with ZipFile(zip_path, "w", ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(folder):
            root_path = Path(root)
            for f in files:
                full_path = root_path / f
                rel_path = full_path.relative_to(folder)
                zf.write(full_path, rel_path)


def upload_webdav(zip_path: Path, webdav_url: str, username: str, password: str, remote_path: str,
                  status_callback=None, log_callback=None):
    # Upload the ZIP file to a WebDAV server using webdavclient3[web:19]
    options = {
        "webdav_hostname": webdav_url.rstrip("/"),
        "webdav_login": username,
        "webdav_password": password,
    }
    client = Client(options)

    remote_dir = os.path.dirname(remote_path)
    if remote_dir and remote_dir not in ("/", "."):
        try:
            if not client.check(remote_dir):
                client.mkdir(remote_dir)
        except Exception:
            pass

    text = f"Uploading {zip_path.name} to WebDAV {webdav_url}{remote_path}"
    if status_callback:
        status_callback(text)
    if log_callback:
        log_callback(text + "\n")
    else:
        print(text)

    client.upload_sync(remote_path=remote_path, local_path=str(zip_path))

    if status_callback:
        status_callback("WebDAV upload complete")
    if log_callback:
        log_callback("WebDAV upload complete\n")
    else:
        print("WebDAV upload complete")


# gui application

class App:
    def __init__(self, root: tk.Tk):
        # Initialize the tkinter UI controls, layout, theming, tabs and logging[web:89]
        self.root = root
        self.root.title("Snapchat Memories Backup")
        self.root.resizable(False, False)

        self.dark_mode = True
        self.bg_dark = "#1e1e1e"
        self.fg_dark = "#ffffff"
        self.entry_bg_dark = "#2b2b2b"
        self.button_bg_dark = "#3c3c3c"
        self.accent_dark = "#007acc"

        self.bg_light = "#f0f0f0"
        self.fg_light = "#000000"
        self.entry_bg_light = "#ffffff"
        self.button_bg_light = "#e0e0e0"
        self.accent_light = "#0063b1"

        self.style = ttk.Style(self.root)
        self.style.theme_use("clam")

        self.json_path_var = tk.StringVar()
        self.output_dir_var = tk.StringVar()
        self.concurrent_var = tk.IntVar(value=16)
        self.webdav_url_var = tk.StringVar()
        self.webdav_user_var = tk.StringVar()
        self.webdav_pass_var = tk.StringVar()
        self.webdav_remote_path_var = tk.StringVar(value="/snapchat_memories_backup.zip")
        self.status_var = tk.StringVar(value="")

        self.webdav_visible = False

        self.create_widgets()
        self.apply_theme()

    def apply_theme(self):
        # Apply light or dark theme colors to widgets[web:89]
        if self.dark_mode:
            bg = self.bg_dark
            fg = self.fg_dark
            entry_bg = self.entry_bg_dark
            button_bg = self.button_bg_dark
            accent = self.accent_dark
        else:
            bg = self.bg_light
            fg = self.fg_light
            entry_bg = self.entry_bg_light
            button_bg = self.button_bg_light
            accent = self.accent_light

        self.root.configure(bg=bg)
        self.style.configure(
            ".",
            background=bg,
            foreground=fg,
            fieldbackground=entry_bg,
        )
        self.style.configure("TLabel", background=bg, foreground=fg)
        self.style.configure("TButton", background=button_bg, foreground=fg)
        self.style.map(
            "TButton",
            background=[("active", accent)],
            foreground=[("active", fg)],
        )
        self.style.configure("TLabelframe", background=bg, foreground=fg)
        self.style.configure("TLabelframe.Label", background=bg, foreground=fg)
        self.style.configure("TNotebook", background=bg)
        self.style.configure("TNotebook.Tab", background=button_bg, foreground=fg)
        self.style.map(
            "TNotebook.Tab",
            background=[("selected", accent)],
            foreground=[("selected", fg)],
        )

        if hasattr(self, "log_text"):
            self.log_text.configure(
                bg=entry_bg,
                fg=fg,
                insertbackground=fg,
            )

    def toggle_theme(self):
        # Toggle between light and dark mode[web:101]
        self.dark_mode = not self.dark_mode
        self.apply_theme()

    def create_widgets(self):
        # Create notebook with main and log tabs[web:119]
        self.notebook = ttk.Notebook(self.root)
        self.notebook.grid(row=0, column=0, sticky="nsew")

        self.main_frame = ttk.Frame(self.notebook, padding=10)
        self.log_frame = ttk.Frame(self.notebook, padding=5)

        self.notebook.add(self.main_frame, text="Main")
        self.notebook.add(self.log_frame, text="Log")

        # Main tab content
        top_bar = ttk.Frame(self.main_frame)
        top_bar.grid(row=0, column=0, columnspan=3, sticky="we")
        ttk.Button(top_bar, text="?", width=3, command=self.show_help).pack(side="left", padx=(0, 10))
        ttk.Button(top_bar, text="Light / Dark", command=self.toggle_theme).pack(side="left")

        row = 1
        ipady_entry = 4  # keep entries a bit taller

        # JSON file selector row
        ttk.Label(self.main_frame, text="memories_history.json").grid(row=row, column=0, sticky="w", padx=5, pady=5)
        self.json_entry = ttk.Entry(self.main_frame, textvariable=self.json_path_var, width=50)
        self.json_entry.grid(row=row, column=1, padx=5, pady=5, ipady=ipady_entry, sticky="we")
        ttk.Button(self.main_frame, text="Browse", command=self.browse_json).grid(
            row=row, column=2, padx=5, pady=5, sticky="we"
        )
        row += 1

        # Output directory row
        ttk.Label(self.main_frame, text="Output folder").grid(row=row, column=0, sticky="w", padx=5, pady=5)
        self.out_entry = ttk.Entry(self.main_frame, textvariable=self.output_dir_var, width=50)
        self.out_entry.grid(row=row, column=1, padx=5, pady=5, ipady=ipady_entry, sticky="we")
        ttk.Button(self.main_frame, text="Browse", command=self.browse_output_dir).grid(
            row=row, column=2, padx=5, pady=5, sticky="we"
        )
        row += 1

        # Concurrency row
        ttk.Label(self.main_frame, text="Maximum parallel downloads").grid(
            row=row, column=0, sticky="w", padx=5, pady=5
        )
        self.concurrent_spin = ttk.Spinbox(
            self.main_frame,
            from_=1,
            to=64,
            textvariable=self.concurrent_var,
            width=6,
        )
        self.concurrent_spin.grid(row=row, column=1, sticky="w", padx=5, pady=5, ipady=ipady_entry)
        row += 1

        # WebDAV toggle button with arrow
        self.webdav_toggle_btn = ttk.Button(
            self.main_frame,
            text="▼ WebDAV options",
            command=self.toggle_webdav
        )
        self.webdav_toggle_btn.grid(row=row, column=0, columnspan=3, sticky="w", padx=5, pady=(10, 0))
        row += 1

        # WebDAV group (start hidden)
        self.webdav_frame = ttk.LabelFrame(self.main_frame, text="WebDAV upload (optional)")
        self.webdav_row_index = row
        row += 1

        # Start button row (left aligned, default size)
        ttk.Button(self.main_frame, text="Start backup", command=self.start_backup).grid(
            row=row, column=0, sticky="w", padx=5, pady=10
        )
        row += 1

        # Status row
        self.status_label = ttk.Label(self.main_frame, textvariable=self.status_var, anchor="w")
        self.status_label.grid(row=row, column=0, columnspan=3, sticky="we", padx=5, pady=5)

        # Build WebDAV controls inside its frame
        self.build_webdav_controls(ipady_entry)

        # Log tab content
        self.log_text = tk.Text(self.log_frame, wrap="word", height=15, width=80)
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        self.log_text.configure(state="disabled")
        scrollbar = ttk.Scrollbar(self.log_frame, command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text["yscrollcommand"] = scrollbar.set

    def build_webdav_controls(self, ipady_entry):
        # Create WebDAV controls inside the frame with consistent spacing and heights
        row_w = 0

        ttk.Label(self.webdav_frame, text="Server URL").grid(row=row_w, column=0, sticky="w", padx=5, pady=3)
        self.url_entry = ttk.Entry(self.webdav_frame, textvariable=self.webdav_url_var, width=50)
        self.url_entry.grid(row=row_w, column=1, padx=5, pady=3, ipady=ipady_entry, sticky="we")
        row_w += 1

        ttk.Label(self.webdav_frame, text="Username").grid(row=row_w, column=0, sticky="w", padx=5, pady=3)
        self.user_entry = ttk.Entry(self.webdav_frame, textvariable=self.webdav_user_var, width=50)
        self.user_entry.grid(row=row_w, column=1, padx=5, pady=3, ipady=ipady_entry, sticky="we")
        row_w += 1

        ttk.Label(self.webdav_frame, text="Password").grid(row=row_w, column=0, sticky="w", padx=5, pady=3)
        self.pass_entry = ttk.Entry(self.webdav_frame, textvariable=self.webdav_pass_var, width=50, show="*")
        self.pass_entry.grid(row=row_w, column=1, padx=5, pady=3, ipady=ipady_entry, sticky="we")
        row_w += 1

        ttk.Label(self.webdav_frame, text="Remote ZIP path").grid(row=row_w, column=0, sticky="w", padx=5, pady=3)
        self.remote_entry = ttk.Entry(self.webdav_frame, textvariable=self.webdav_remote_path_var, width=50)
        self.remote_entry.grid(row=row_w, column=1, padx=5, pady=3, ipady=ipady_entry, sticky="we")
        row_w += 1

    def toggle_webdav(self):
        # Show or hide the WebDAV options section with arrow indicator
        if self.webdav_visible:
            self.webdav_frame.grid_forget()
            self.webdav_visible = False
            self.webdav_toggle_btn.config(text="▼ WebDAV options")
        else:
            self.webdav_frame.grid(
                row=self.webdav_row_index, column=0, columnspan=3,
                sticky="we", padx=5, pady=10
            )
            self.webdav_visible = True
            self.webdav_toggle_btn.config(text="▲ WebDAV options")

    def browse_json(self):
        # Open file chooser to select memories_history.json
        path = filedialog.askopenfilename(
            title="Select memories_history.json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if path:
            self.json_path_var.set(path)

    def browse_output_dir(self):
        # Open directory chooser for output directory
        path = filedialog.askdirectory(
            title="Select output folder"
        )
        if path:
            self.output_dir_var.set(path)

    def set_status(self, text: str):
        # Thread-safe update of status label
        def _update():
            self.status_var.set(text)
        self.root.after(0, _update)

    def append_log(self, text: str):
        # Append text to the log tab[web:111]
        def _append():
            self.log_text.configure(state="normal")
            self.log_text.insert("end", text)
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        self.root.after(0, _append)

    def validate_inputs(self):
        # Validate required UI inputs before starting the backup
        json_path = self.json_path_var.get().strip()
        if not json_path:
            raise ValueError("Please select memories_history.json")
        if not Path(json_path).exists():
            raise ValueError("Selected JSON file does not exist")

        output_dir = self.output_dir_var.get().strip()
        if output_dir:
            output_root = Path(output_dir)
        else:
            output_root = build_output_dir()

        concurrent_downloads = int(self.concurrent_var.get())
        if concurrent_downloads < 1:
            concurrent_downloads = 1

        webdav_url = self.webdav_url_var.get().strip()
        webdav_user = self.webdav_user_var.get().strip()
        webdav_pass = self.webdav_pass_var.get().strip()
        webdav_remote_path = self.webdav_remote_path_var.get().strip()

        if webdav_url and (not webdav_user or not webdav_pass):
            raise ValueError("If WebDAV URL is set, username and password must also be provided")

        return {
            "json_path": Path(json_path),
            "output_root": output_root,
            "concurrent": concurrent_downloads,
            "webdav_url": webdav_url if webdav_url else None,
            "webdav_user": webdav_user if webdav_user else None,
            "webdav_pass": webdav_pass if webdav_pass else None,
            "webdav_remote_path": webdav_remote_path if webdav_remote_path else "/snapchat_memories_backup.zip",
        }

    def start_backup(self):
        # Start backup in a background thread to keep the UI responsive
        try:
            config = self.validate_inputs()
        except Exception as e:
            messagebox.showerror("Input error", str(e))
            return

        self.set_status("Starting backup")
        self.append_log("Starting backup\n")
        threading.Thread(target=self.run_backup, args=(config,), daemon=True).start()

    def run_backup(self, config: dict):
        # Perform the download, zip, and optional WebDAV upload steps
        try:
            json_path = config["json_path"]
            output_root = config["output_root"]
            concurrent_downloads = config["concurrent"]

            ensure_dir(output_root)

            self.set_status("Loading memories JSON")
            self.append_log("Loading memories JSON\n")
            memories = read_memories_json(json_path)

            downloader = MemoryDownloader(
                memories,
                output_root,
                max_workers=concurrent_downloads,
                status_callback=self.set_status,
                log_callback=self.append_log,
            )
            downloader.download_all()

            zip_name = f"{output_root.name}.zip"
            zip_path = output_root.parent / zip_name
            zip_folder(output_root, zip_path, status_callback=self.set_status, log_callback=self.append_log)
            self.set_status(f"ZIP created at {zip_path}")
            self.append_log(f"ZIP created at {zip_path}\n")

            if config["webdav_url"] and config["webdav_user"] and config["webdav_pass"]:
                upload_webdav(
                    zip_path=zip_path,
                    webdav_url=config["webdav_url"],
                    username=config["webdav_user"],
                    password=config["webdav_pass"],
                    remote_path=config["webdav_remote_path"],
                    status_callback=self.set_status,
                    log_callback=self.append_log,
                )

            self.set_status("Backup completed successfully")
            self.append_log("Backup completed successfully\n")
            messagebox.showinfo("Done", "Backup completed successfully")

        except Exception as e:
            self.set_status(f"Error {e}")
            self.append_log(f"Error {e}\n")
            messagebox.showerror("Error", f"Backup failed: {e}")

    def show_help(self):
        # Show a simple tutorial dialog explaining how to get the JSON file and what to select[web:181]
        text = (
            "How to get memories_history.json\n\n"
            "1. Open Snapchat and go to your profile\n"
            "2. Tap the settings gear, then 'My Data'\n"
            "3. Request your data with 'JSON' selected and wait for the email\n"
            "4. Download the ZIP file and extract it on your computer\n"
            "5. Inside the extracted folder, find 'memories_history.json'\n\n"
            "In this app\n"
            "- Click 'Browse' next to 'memories_history.json' and select that file\n"
            "- Choose an output folder (or leave empty to use a timestamped folder on your Desktop)\n"
            "- Optionally open 'WebDAV options' and fill in server URL, username, password,\n"
            "  and the remote ZIP path if you want the backup uploaded automatically\n"
            "- Set 'Maximum parallel downloads' higher for faster downloads if your connection can handle it\n"
            "- Click 'Start backup' and watch the details in the 'Log' tab"
        )
        messagebox.showinfo("Help", text)


# entry point

def main():
    # Launch the tkinter GUI application
    root = tk.Tk()
    app = App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
