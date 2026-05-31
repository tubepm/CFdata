package com.cfdata

import android.content.Context
import android.content.SharedPreferences
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.KeyEvent
import android.view.View
import android.webkit.JavascriptInterface
import android.webkit.WebChromeClient
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.appcompat.app.AppCompatActivity
import com.cfdata.cfdata.Cfdata

class MainActivity : AppCompatActivity() {
    private lateinit var webView: WebView
    private lateinit var prefs: SharedPreferences

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        prefs = getSharedPreferences("cfdata_prefs", Context.MODE_PRIVATE)
        val defaultUrl = "speed.cloudflare.com/__down?bytes=100000000"
        val savedUrl = prefs.getString("speed_test_url", defaultUrl) ?: defaultUrl

        val port = 13335
        val dataDir = cacheDir.absolutePath
        val thread = Thread {
            try {
                Cfdata.setDataDir(dataDir)
                Cfdata.startServer(port.toLong(), savedUrl)
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
        thread.start()

        webView = findViewById(R.id.webView)
        val settings = webView.settings
        settings.javaScriptEnabled = true
        settings.domStorageEnabled = true
        settings.loadWithOverviewMode = true
        settings.useWideViewPort = true

        WebView.setWebContentsDebuggingEnabled(true)

        webView.addJavascriptInterface(WebAppInterface(prefs), "Android")

        webView.webChromeClient = WebChromeClient()
        webView.webViewClient = object : WebViewClient() {}

        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.O_MR1) {
            webView.settings.safeBrowsingEnabled = false
        }

        if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.LOLLIPOP) {
            webView.settings.mixedContentMode = android.webkit.WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
        }

        waitForServerAndLoad(port)
    }

    override fun onBackPressed() {
        if (this::webView.isInitialized && webView.canGoBack()) {
            webView.goBack()
        } else {
            android.app.AlertDialog.Builder(this)
                .setMessage("确定要退出应用吗？")
                .setPositiveButton("取消", null)
                .setNegativeButton("退出") { _, _ ->
                    finishAffinity()
                    android.os.Process.killProcess(android.os.Process.myPid())
                }
                .show()
        }
    }

    private fun waitForServerAndLoad(port: Int) {
        Thread {
            val maxAttempts = 30
            for (i in 0 until maxAttempts) {
                try {
                    val url = java.net.URL("http://127.0.0.1:$port/")
                    val conn = url.openConnection() as java.net.HttpURLConnection
                    conn.connectTimeout = 1000
                    conn.readTimeout = 1000
                    conn.requestMethod = "GET"
                    conn.inputStream.use { }
                    conn.disconnect()
                    break
                } catch (e: Exception) {
                    Thread.sleep(500)
                }
            }
            Handler(Looper.getMainLooper()).post {
                webView.loadUrl("http://127.0.0.1:$port/")
            }
        }.start()
    }

    private class WebAppInterface(private val prefs: SharedPreferences) {
        @JavascriptInterface
        fun saveSpeedUrl(url: String) {
            prefs.edit().putString("speed_test_url", url).apply()
        }
    }
}
