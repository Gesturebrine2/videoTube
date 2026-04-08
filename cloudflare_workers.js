// ==================== CLOUDFLARE WORKER + D1 + R2 ====================
// Architecture: Cloudflare Workers + D1 (SQLite) + R2 (Storage)

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const path = url.pathname;
        const method = request.method;

        // CORS Headers
        const corsHeaders = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Session-Id",
        };

        // Handle preflight
        if (method === "OPTIONS") {
            return new Response(null, { status: 204, headers: corsHeaders });
        }

        // Response helpers
        const jsonResponse = (data, status = 200) => 
            new Response(JSON.stringify(data), { 
                status, 
                headers: { ...corsHeaders, "Content-Type": "application/json" } 
            });

        const errorResponse = (message, status = 400) => 
            jsonResponse({ error: message, status, timestamp: new Date().toISOString() }, status);

        // Auth check
        const checkAuth = () => {
            const auth = request.headers.get("Authorization")?.replace("Bearer ", "");
            return auth === env.ADMIN_TOKEN;
        };

        const getSessionId = () => 
            request.headers.get("X-Session-Id") || crypto.randomUUID();

        const getClientIP = () => 
            request.headers.get("X-Forwarded-For")?.split(",")[0] || "unknown";

        try {
            // ==================== PUBLIC ENDPOINTS ====================

            // Health check
            if (path === "/health" && method === "GET") {
                return jsonResponse({ 
                    status: "ok", 
                    ts: Date.now(), 
                    platform: "cloudflare-all-in-one"
                });
            }

            // Public stats
            if (path === "/api/stats/public" && method === "GET") {
                const videoCount = await env.DB.prepare(
                    "SELECT COUNT(*) as count FROM videos WHERE status = 'active'"
                ).first();
                const shortCount = await env.DB.prepare(
                    "SELECT COUNT(*) as count FROM shorts WHERE status = 'active'"
                ).first();
                return jsonResponse({ 
                    totalVideos: videoCount?.count || 0, 
                    totalShorts: shortCount?.count || 0 
                });
            }

            // Get videos
            if (path === "/api/videos" && method === "GET") {
                const limit = Math.min(parseInt(url.searchParams.get("limit")) || 50, 100);
                const { results } = await env.DB.prepare(
                    "SELECT * FROM videos WHERE status = 'active' ORDER BY addedAt DESC LIMIT ?"
                ).bind(limit).all();
                return jsonResponse(results || []);
            }

            // Get single video
            if (path.match(/^\/api\/video\/[a-zA-Z0-9_-]+$/) && method === "GET") {
                const id = path.split("/")[3];
                const video = await env.DB.prepare(
                    "SELECT * FROM videos WHERE (numericId = ? OR id = ?) AND status = 'active'"
                ).bind(id, id).first();
                
                if (!video) return errorResponse("Video not found", 404);
                
                // Increment views (fire and forget)
                env.DB.prepare("UPDATE videos SET views = views + 1 WHERE numericId = ?").bind(id).run();
                
                return jsonResponse(video);
            }

            // Get shorts
            if (path === "/api/shorts" && method === "GET") {
                const limit = Math.min(parseInt(url.searchParams.get("limit")) || 50, 100);
                const { results } = await env.DB.prepare(
                    "SELECT * FROM shorts WHERE status = 'active' ORDER BY addedAt DESC LIMIT ?"
                ).bind(limit).all();
                return jsonResponse(results || []);
            }

            // Get single short
            if (path.match(/^\/api\/short\/[a-zA-Z0-9_-]+$/) && method === "GET") {
                const id = path.split("/")[3];
                const short = await env.DB.prepare(
                    "SELECT * FROM shorts WHERE (numericId = ? OR id = ?) AND status = 'active'"
                ).bind(id, id).first();
                
                if (!short) return errorResponse("Short not found", 404);
                
                env.DB.prepare("UPDATE shorts SET views = views + 1 WHERE numericId = ?").bind(id).run();
                return jsonResponse(short);
            }

            // ==================== RECOMMENDED SHORTS (WITH ALGORITHM) ====================
            if (path === "/api/shorts/recommend" && method === "GET") {
                const sessionId = url.searchParams.get("sessionId") || getSessionId();
                const limit = parseInt(url.searchParams.get("limit")) || 20;
                
                // Step 1: Get user's session history
                let userHistory = { tags: {}, categories: {}, watchedIds: [] };
                try {
                    const history = await env.DB.prepare(`
                        SELECT tags, category, shortId FROM session_history 
                        WHERE sessionId = ? 
                        ORDER BY watchedAt DESC 
                        LIMIT 15
                    `).bind(sessionId).all();
                    
                    if (history.results && history.results.length > 0) {
                        history.results.forEach((item, index) => {
                            const weight = Math.max(0.1, 1 - (index * 0.06));
                            if (item.tags) {
                                try {
                                    const tags = JSON.parse(item.tags);
                                    tags.forEach(tag => {
                                        userHistory.tags[tag] = (userHistory.tags[tag] || 0) + weight;
                                    });
                                } catch (e) {}
                            }
                            if (item.category) {
                                userHistory.categories[item.category] = 
                                    (userHistory.categories[item.category] || 0) + weight;
                            }
                            userHistory.watchedIds.push(item.shortId);
                        });
                    }
                } catch (e) {
                    console.error("History fetch error:", e);
                }

                // Step 2: Fetch candidate shorts
                const { results } = await env.DB.prepare(`
                    SELECT *, 
                           (likes * 2 + shares * 3) as engagementWeight,
                           julianday('now') - julianday(uploadDate) as ageDays
                    FROM shorts 
                    WHERE status = 'active' AND views > 0 
                    ORDER BY addedAt DESC 
                    LIMIT 100
                `).all();

                if (!results || results.length === 0) {
                    return jsonResponse([]);
                }

                // Step 3: Score each short
                const scored = results
                    .filter(short => !userHistory.watchedIds.includes(short.numericId))
                    .map(short => {
                        let tagScore = 0;
                        let categoryScore = 0;
                        
                        let shortTags = [];
                        try {
                            shortTags = short.tags ? JSON.parse(short.tags) : [];
                        } catch (e) {}
                        
                        if (shortTags.length > 0 && Object.keys(userHistory.tags).length > 0) {
                            shortTags.forEach(tag => {
                                if (userHistory.tags[tag]) {
                                    tagScore += userHistory.tags[tag];
                                }
                            });
                            tagScore = Math.min(tagScore / shortTags.length, 1);
                        } else {
                            tagScore = 0.5;
                        }
                        
                        if (short.category && userHistory.categories[short.category]) {
                            categoryScore = Math.min(userHistory.categories[short.category], 1);
                        } else {
                            categoryScore = 0.5;
                        }
                        
                        const engagementScore = Math.min(
                            ((short.likes || 0) * 2 + (short.shares || 0) * 3) / ((short.views || 1) + 1) / 0.5,
                            1
                        );
                        
                        const recencyScore = short.ageDays <= 7 ? 0.9 : 
                                            short.ageDays <= 30 ? 0.7 : 
                                            short.ageDays <= 90 ? 0.5 : 0.3;
                        
                        const diversityScore = Math.random();
                        
                        const hasHistory = Object.keys(userHistory.tags).length > 0;
                        
                        const finalScore = hasHistory ? 
                            (tagScore * 0.50) + 
                            (categoryScore * 0.20) + 
                            (engagementScore * 0.15) + 
                            (recencyScore * 0.10) + 
                            (diversityScore * 0.05) :
                            (engagementScore * 0.40) + 
                            (recencyScore * 0.25) + 
                            (diversityScore * 0.35);
                        
                        return {
                            ...short,
                            score: finalScore,
                            tagScore,
                            categoryScore,
                            engagementScore,
                            recencyScore
                        };
                    });

                // Step 4: Apply diversity rules
                const diversified = [];
                const tagCount = {};
                const categoryCount = {};
                const MAX_SAME_TAG = 3;
                const MAX_SAME_CATEGORY = 4;
                
                for (const short of scored) {
                    let shortTags = [];
                    try {
                        shortTags = short.tags ? JSON.parse(short.tags) : [];
                    } catch (e) {}
                    
                    let maxTagCount = 0;
                    shortTags.forEach(tag => {
                        tagCount[tag] = (tagCount[tag] || 0) + 1;
                        maxTagCount = Math.max(maxTagCount, tagCount[tag]);
                    });
                    
                    categoryCount[short.category || 'uncategorized'] = 
                        (categoryCount[short.category || 'uncategorized'] || 0) + 1;
                    
                    if (maxTagCount > MAX_SAME_TAG) continue;
                    if (categoryCount[short.category || 'uncategorized'] > MAX_SAME_CATEGORY) continue;
                    
                    if ((diversified.length + 1) % 5 === 0 && short.views < 1000) {
                        const popularShort = scored.find(s => s.views >= 1000 && !diversified.includes(s));
                        if (popularShort) {
                            diversified.push(popularShort);
                        }
                    }
                    
                    diversified.push(short);
                }

                const recommendations = diversified
                    .sort((a, b) => b.score - a.score)
                    .slice(0, limit);

                return jsonResponse(recommendations);
            }

            // Get tags
            if (path === "/api/tags" && method === "GET") {
                const { results } = await env.DB.prepare(
                    "SELECT * FROM tags ORDER BY usageCount DESC LIMIT 100"
                ).all();
                return jsonResponse(results || []);
            }

            // ==================== PROTECTED ENDPOINTS ====================

            if (!checkAuth()) {
                return errorResponse("Unauthorized", 401);
            }

            // Admin stats
            if (path === "/api/stats" && method === "GET") {
                const videoCount = await env.DB.prepare("SELECT COUNT(*) as count FROM videos WHERE status = 'active'").first();
                const shortCount = await env.DB.prepare("SELECT COUNT(*) as count FROM shorts WHERE status = 'active'").first();
                const tagCount = await env.DB.prepare("SELECT COUNT(*) as count FROM tags").first();
                const totalViews = await env.DB.prepare("SELECT COALESCE(SUM(views), 0) as views FROM videos WHERE status = 'active'").first();
                
                return jsonResponse({
                    totalVideos: videoCount?.count || 0,
                    totalShorts: shortCount?.count || 0,
                    totalTags: tagCount?.count || 0,
                    totalViews: totalViews?.views || 0
                });
            }

            // Upload video metadata
            if (path === "/api/upload/video" && method === "POST") {
                const data = await request.json().catch(() => ({}));
                
                if (!data.title || !data.videoUrl) {
                    return errorResponse("Title and videoUrl required", 400);
                }

                const numericIdResult = await env.DB.prepare(
                    "SELECT MAX(CAST(numericId AS INTEGER)) as maxId FROM videos"
                ).first();
                const maxId = numericIdResult?.maxId || 0;
                const numericId = String(maxId + 1).padStart(6, "0");
                const urlFriendlyId = data.title.toLowerCase().replace(/[^a-z0-9]+/g, "-").substring(0, 50) || `video-${numericId}`;
                const now = new Date().toISOString();

                await env.DB.prepare(`
                    INSERT INTO videos (id, numericId, title, videoUrl, thumbnail, duration, category, tags, uploadDate, type, views, status, addedAt, updatedAt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `).bind(
                    urlFriendlyId, numericId, data.title, data.videoUrl, 
                    data.thumbnail || "", data.duration || "0:00", 
                    data.category || "uncategorized", JSON.stringify(data.tags || []),
                    data.uploadDate || now.split("T")[0], 'r2', 0, 'active', now, now
                ).run();

                if (data.tags && Array.isArray(data.tags)) {
                    for (const tag of data.tags) {
                        await env.DB.prepare(
                            "INSERT INTO tags (name, usageCount) VALUES (?, 1) ON CONFLICT(name) DO UPDATE SET usageCount = usageCount + 1"
                        ).bind(tag.toLowerCase()).run();
                    }
                }

                return jsonResponse({ success: true, numericId, id: urlFriendlyId });
            }

            // Upload short metadata
            if (path === "/api/upload/short" && method === "POST") {
                const data = await request.json().catch(() => ({}));
                
                if (!data.title || !data.videoUrl) {
                    return errorResponse("Title and videoUrl required", 400);
                }

                const numericIdResult = await env.DB.prepare(
                    "SELECT MAX(CAST(numericId AS INTEGER)) as maxId FROM shorts"
                ).first();
                const maxId = numericIdResult?.maxId || 0;
                const numericId = String(maxId + 1).padStart(6, "0");
                const now = new Date().toISOString();

                await env.DB.prepare(`
                    INSERT INTO shorts (id, numericId, title, videoUrl, thumbnail, duration, tags, views, likes, shares, engagementScore, status, uploadDate, addedAt, updatedAt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0.0, 'active', ?, ?, ?)
                `).bind(
                    `short-${numericId}`, numericId, data.title, data.videoUrl,
                    data.thumbnail || "", data.duration || "0:00",
                    JSON.stringify(data.tags || []), data.uploadDate || now.split("T")[0], now, now
                ).run();

                if (data.tags && Array.isArray(data.tags)) {
                    for (const tag of data.tags) {
                        await env.DB.prepare(
                            "INSERT INTO tags (name, usageCount) VALUES (?, 1) ON CONFLICT(name) DO UPDATE SET usageCount = usageCount + 1"
                        ).bind(tag.toLowerCase()).run();
                    }
                }

                return jsonResponse({ success: true, numericId });
            }

            // ==================== R2 FILE UPLOAD ====================
            if (path === "/api/upload/file" && method === "POST") {
                const formData = await request.formData();
                const file = formData.get("file");
                const storagePath = formData.get("path");
                const filename = formData.get("filename");

                if (!file || !storagePath || !filename) {
                    return errorResponse("file, path, and filename required", 400);
                }

                if (!env.BUCKET) {
                    return errorResponse("R2 Bucket not configured", 500);
                }

                try {
                    // Upload to R2
                    await env.BUCKET.put(`${storagePath}/${filename}`, file.stream(), {
                        httpMetadata: {
                            contentType: file.type || "application/octet-stream"
                        }
                    });

                    // Construct public URL
                    // Format: https://<bucket-name>.<account-id>.r2.dev/<path>/<filename>
                    // You can also use a custom domain if configured
                    const publicUrl = `https://${env.R2_PUBLIC_URL}/${storagePath}/${filename}`;

                    return jsonResponse({ success: true, url: publicUrl });
                } catch (error) {
                    console.error("R2 upload error:", error);
                    return errorResponse("Upload failed: " + error.message, 500);
                }
            }

            // Delete video
            if (path === "/api/video/delete" && method === "DELETE") {
                const { id } = await request.json().catch(() => ({}));
                if (!id) return errorResponse("Video ID required", 400);
                
                await env.DB.prepare(
                    "UPDATE videos SET status = 'removed', updatedAt = datetime('now') WHERE numericId = ? OR id = ?"
                ).bind(id, id).run();
                
                return jsonResponse({ success: true, message: "Video removed" });
            }

            // Delete short
            if (path === "/api/short/delete" && method === "DELETE") {
                const { id } = await request.json().catch(() => ({}));
                if (!id) return errorResponse("Short ID required", 400);
                
                await env.DB.prepare(
                    "UPDATE shorts SET status = 'removed', updatedAt = datetime('now') WHERE numericId = ? OR id = ?"
                ).bind(id, id).run();
                
                return jsonResponse({ success: true, message: "Short removed" });
            }

            // Update video
            if (path === "/api/video/update" && method === "PUT") {
                const { id, title, category, tags } = await request.json().catch(() => ({}));
                if (!id) return errorResponse("Video ID required", 400);

                const updates = [];
                const params = [];
                
                if (title) { updates.push("title = ?"); params.push(title); }
                if (category) { updates.push("category = ?"); params.push(category); }
                if (tags) { updates.push("tags = ?"); params.push(JSON.stringify(tags)); }
                
                if (updates.length === 0) return errorResponse("No fields to update", 400);
                
                params.push(id);
                await env.DB.prepare(
                    `UPDATE videos SET ${updates.join(", ")}, updatedAt = datetime('now') WHERE numericId = ? OR id = ?`
                ).bind(...params, id).run();
                
                return jsonResponse({ success: true, message: "Video updated" });
            }

            // Track like
            if (path === "/api/short/like" && method === "POST") {
                const { shortId } = await request.json().catch(() => ({}));
                const sessionId = getSessionId();
                
                if (!shortId) return errorResponse("Short ID required", 400);

                const existing = await env.DB.prepare(
                    "SELECT * FROM short_interactions WHERE shortId = ? AND sessionId = ? AND action = 'like'"
                ).bind(shortId, sessionId).first();

                if (existing) {
                    await env.DB.prepare("DELETE FROM short_interactions WHERE id = ?").bind(existing.id).run();
                    await env.DB.prepare("UPDATE shorts SET likes = MAX(likes - 1, 0) WHERE numericId = ?").bind(shortId).run();
                    return jsonResponse({ success: true, action: "unliked" });
                } else {
                    await env.DB.prepare(
                        "INSERT INTO short_interactions (shortId, sessionId, action, ipAddress) VALUES (?, ?, 'like', ?)"
                    ).bind(shortId, sessionId, getClientIP()).run();
                    await env.DB.prepare("UPDATE shorts SET likes = likes + 1 WHERE numericId = ?").bind(shortId).run();
                    return jsonResponse({ success: true, action: "liked" });
                }
            }

            // Track share
            if (path === "/api/short/share" && method === "POST") {
                const { shortId } = await request.json().catch(() => ({}));
                const sessionId = getSessionId();
                
                if (!shortId) return errorResponse("Short ID required", 400);

                await env.DB.prepare(
                    "INSERT INTO short_interactions (shortId, sessionId, action, ipAddress) VALUES (?, ?, 'share', ?)"
                ).bind(shortId, sessionId, getClientIP()).run();
                await env.DB.prepare("UPDATE shorts SET shares = shares + 1 WHERE numericId = ?").bind(shortId).run();
                
                return jsonResponse({ success: true, action: "shared" });
            }

            // Track view (HYBRID)
            if (path === "/api/short/view" && method === "POST") {
                const { shortId, watchDuration, watchTime } = await request.json().catch(() => ({}));
                const sessionId = getSessionId();
                
                if (!shortId) return errorResponse("Short ID required", 400);

                const shouldTrack = watchDuration >= 0.5 || watchTime >= 15 || watchDuration >= 0.9;
                
                if (!shouldTrack) {
                    return jsonResponse({ success: true, tracked: false, reason: "threshold not met" });
                }

                const action = watchDuration >= 0.9 ? 'complete' : 'view';

                await env.DB.prepare(
                    "INSERT INTO short_interactions (shortId, sessionId, action, metadata, ipAddress) VALUES (?, ?, ?, ?, ?)"
                ).bind(shortId, sessionId, action, JSON.stringify({ watchDuration, watchTime }), getClientIP()).run();

                try {
                    const short = await env.DB.prepare(
                        "SELECT tags, category FROM shorts WHERE numericId = ?"
                    ).bind(shortId).first();
                    
                    if (short) {
                        await env.DB.prepare(`
                            INSERT INTO session_history (sessionId, shortId, tags, category, watchDuration, watchedAt)
                            VALUES (?, ?, ?, ?, ?, datetime('now'))
                            ON CONFLICT(sessionId, shortId) DO UPDATE SET 
                                watchDuration = ?,
                                watchedAt = datetime('now')
                        `).bind(
                            sessionId, shortId, short.tags, short.category, watchDuration, watchDuration
                        ).run();
                    }
                } catch (e) {
                    console.error("Session history update error:", e);
                }
                
                return jsonResponse({ success: true, tracked: true, action });
            }

            // Batch track views
            if (path === "/api/short/view/batch" && method === "POST") {
                const { views } = await request.json().catch(() => ({}));
                const sessionId = getSessionId();
                
                if (!views || !Array.isArray(views)) {
                    return errorResponse("views array required", 400);
                }

                const results = [];
                
                for (const view of views) {
                    try {
                        const { shortId, watchDuration, watchTime } = view;
                        if (!shortId) continue;
                        
                        const shouldTrack = watchDuration >= 0.5 || watchTime >= 15 || watchDuration >= 0.9;
                        
                        if (shouldTrack) {
                            const action = watchDuration >= 0.9 ? 'complete' : 'view';
                            
                            await env.DB.prepare(
                                "INSERT INTO short_interactions (shortId, sessionId, action, metadata, ipAddress) VALUES (?, ?, ?, ?, ?)"
                            ).bind(shortId, sessionId, action, JSON.stringify({ watchDuration, watchTime }), getClientIP()).run();
                            
                            const short = await env.DB.prepare(
                                "SELECT tags, category FROM shorts WHERE numericId = ?"
                            ).bind(shortId).first();
                            
                            if (short) {
                                await env.DB.prepare(`
                                    INSERT INTO session_history (sessionId, shortId, tags, category, watchDuration, watchedAt)
                                    VALUES (?, ?, ?, ?, ?, datetime('now'))
                                    ON CONFLICT(sessionId, shortId) DO UPDATE SET 
                                        watchDuration = ?,
                                        watchedAt = datetime('now')
                                `).bind(
                                    sessionId, shortId, short.tags, short.category, watchDuration, watchDuration
                                ).run();
                            }
                            
                            results.push({ shortId, tracked: true });
                        } else {
                            results.push({ shortId, tracked: false });
                        }
                    } catch (e) {
                        results.push({ shortId, tracked: false, error: e.message });
                    }
                }
                
                return jsonResponse({ success: true, results });
            }

            // Add tag
            if (path === "/api/tags/add" && method === "POST") {
                const { name } = await request.json().catch(() => ({}));
                if (!name) return errorResponse("Tag name required", 400);
                
                await env.DB.prepare(
                    "INSERT INTO tags (name, usageCount) VALUES (?, 0) ON CONFLICT(name) DO NOTHING"
                ).bind(name.toLowerCase()).run();
                
                return jsonResponse({ success: true, message: "Tag added" });
            }

            // Delete tag
            if (path === "/api/tags/delete" && method === "DELETE") {
                const { name } = await request.json().catch(() => ({}));
                if (!name) return errorResponse("Tag name required", 400);
                
                await env.DB.prepare("DELETE FROM tags WHERE name = ?").bind(name.toLowerCase()).run();
                
                return jsonResponse({ success: true, message: "Tag deleted" });
            }

            // 404 for unknown routes
            return errorResponse("Endpoint not found", 404);

        } catch (error) {
            console.error("Worker error:", error);
            return errorResponse("Internal server error", 500);
        }
    }
};
