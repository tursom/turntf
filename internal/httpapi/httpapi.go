package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"notifier/internal/security"
	"notifier/internal/store"
)

type API struct {
	store          *store.Store
	userSessionTTL time.Duration
	mux            *http.ServeMux
}

func New(st *store.Store, userSessionTTL time.Duration) *API {
	api := &API{
		store:          st,
		userSessionTTL: userSessionTTL,
		mux:            http.NewServeMux(),
	}
	api.routes()
	return api
}

func (a *API) Handler() http.Handler {
	return loggingMiddleware(a.mux)
}

func (a *API) routes() {
	a.mux.HandleFunc("GET /", a.handleHome)
	a.mux.HandleFunc("GET /login", a.handleLoginPage)
	a.mux.HandleFunc("POST /login", a.handleLoginPageSubmit)
	a.mux.HandleFunc("POST /logout", a.handleLogout)
	a.mux.HandleFunc("GET /dashboard", a.handleDashboard)
	a.mux.HandleFunc("GET /healthz", a.handleHealth)
	a.mux.HandleFunc("POST /api/v1/users/register", a.handleRegister)
	a.mux.HandleFunc("POST /api/v1/users/login", a.handleLogin)
	a.mux.HandleFunc("GET /api/v1/users/me", a.handleMe)
	a.mux.HandleFunc("GET /api/v1/notifications", a.handleListNotifications)
	a.mux.HandleFunc("POST /api/v1/push", a.handlePush)
	a.mux.HandleFunc("GET /api/v1/admin/users", a.handleAdminListUsers)
	a.mux.HandleFunc("POST /api/v1/admin/users", a.handleAdminCreateUser)
	a.mux.HandleFunc("PATCH /api/v1/admin/users/{id}", a.handleAdminUpdateUser)
	a.mux.HandleFunc("DELETE /api/v1/admin/users/{id}", a.handleAdminDeleteUser)
}

type registerRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type adminCreateUserRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role"`
}

type adminUpdateUserRequest struct {
	Role     *string `json:"role,omitempty"`
	Password *string `json:"password,omitempty"`
}

type loginResponse struct {
	Token     string     `json:"token"`
	ExpiresAt time.Time  `json:"expires_at"`
	User      store.User `json:"user"`
}

type pushRequest struct {
	PushID   string          `json:"push_id"`
	Sender   string          `json:"sender"`
	Title    string          `json:"title"`
	Body     string          `json:"body"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleHome(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.userFromCookie(r); ok {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (a *API) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.userFromCookie(r); ok {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	a.renderLoginPage(w, http.StatusOK, loginPageData{})
}

func (a *API) handleLoginPageSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.renderLoginPage(w, http.StatusBadRequest, loginPageData{Error: "invalid form submission"})
		return
	}

	username := strings.TrimSpace(r.FormValue("username"))
	password := r.FormValue("password")

	session, err := a.authenticateAndCreateSession(r.Context(), username, password)
	if err != nil {
		status := http.StatusUnauthorized
		if errors.Is(err, errInvalidInput) {
			status = http.StatusBadRequest
		}
		a.renderLoginPage(w, status, loginPageData{
			Error:    err.Error(),
			Username: username,
		})
		return
	}

	setSessionCookie(w, session.Token, session.ExpiresAt)
	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

func (a *API) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateUserCredentials(req.Username, req.Password); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	passwordHash, err := security.HashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to hash password")
		return
	}

	user, err := a.store.CreateInitialAdmin(r.Context(), req.Username, passwordHash)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrRegistrationClosed):
			writeError(w, http.StatusForbidden, "public registration is closed")
		case errors.Is(err, store.ErrConflict):
			writeError(w, http.StatusConflict, "username already exists")
		default:
			writeError(w, http.StatusInternalServerError, "failed to create user")
		}
		return
	}

	writeJSON(w, http.StatusCreated, user)
}

func (a *API) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	session, err := a.authenticateAndCreateSession(r.Context(), req.Username, req.Password)
	if err != nil {
		if errors.Is(err, errInvalidInput) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if errors.Is(err, errInvalidAuth) {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create session")
		return
	}

	writeJSON(w, http.StatusOK, loginResponse{
		Token:     session.Token,
		ExpiresAt: session.ExpiresAt,
		User:      session.User,
	})
}

func (a *API) handleMe(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireUser(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, user)
}

func (a *API) handleListNotifications(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireUser(w, r)
	if !ok {
		return
	}

	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		limit = parsed
	}

	items, err := a.store.ListNotificationsByUser(r.Context(), user.ID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list notifications")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

func (a *API) handlePush(w http.ResponseWriter, r *http.Request) {
	serviceKey, ok := a.requireService(w, r)
	if !ok {
		return
	}

	var req pushRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validatePushRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	user, err := a.store.GetUserByPushID(r.Context(), req.PushID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "push_id not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to query target user")
		return
	}

	metadata := strings.TrimSpace(string(req.Metadata))
	notification, err := a.store.CreateNotification(r.Context(), user.ID, req.PushID, req.Sender, req.Title, req.Body, metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to store notification")
		return
	}

	if err := a.store.TouchServiceKeyUsage(r.Context(), serviceKey.ID); err != nil {
		log.Printf("warn: failed to update service key usage: %v", err)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":          "accepted",
		"notification_id": notification.ID,
		"push_id":         notification.PushID,
		"created_at":      notification.CreatedAt,
	})
}

func (a *API) handleAdminListUsers(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requireAdmin(w, r); !ok {
		return
	}

	users, err := a.store.ListUsers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list users")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": users,
		"count": len(users),
	})
}

func (a *API) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requireAdmin(w, r); !ok {
		return
	}

	var req adminCreateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateUserCredentials(req.Username, req.Password); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	role, err := validateRole(req.Role)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	passwordHash, err := security.HashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to hash password")
		return
	}

	user, err := a.store.CreateUser(r.Context(), req.Username, passwordHash, role)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrConflict):
			writeError(w, http.StatusConflict, "username already exists")
		case errors.Is(err, store.ErrInvalidRole):
			writeError(w, http.StatusBadRequest, "role must be admin or user")
		default:
			writeError(w, http.StatusInternalServerError, "failed to create user")
		}
		return
	}

	writeJSON(w, http.StatusCreated, user)
}

func (a *API) handleAdminUpdateUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requireAdmin(w, r); !ok {
		return
	}

	userID, ok := parsePathUserID(w, r)
	if !ok {
		return
	}

	var req adminUpdateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	hasRoleUpdate := req.Role != nil
	hasPasswordUpdate := req.Password != nil
	if !hasRoleUpdate && !hasPasswordUpdate {
		writeError(w, http.StatusBadRequest, "at least one of role or password must be provided")
		return
	}

	var normalizedRole string
	if hasRoleUpdate {
		var err error
		normalizedRole, err = validateRole(*req.Role)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	var passwordHash string
	if hasPasswordUpdate {
		if err := validatePassword(*req.Password); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		var err error
		passwordHash, err = security.HashPassword(*req.Password)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to hash password")
			return
		}
	}

	targetUser, err := a.store.GetUserByID(r.Context(), userID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load user")
		return
	}

	if hasRoleUpdate && normalizedRole != targetUser.Role && targetUser.Role == store.UserRoleAdmin && normalizedRole != store.UserRoleAdmin {
		if err := a.ensureUserIsNotLastAdmin(r.Context(), targetUser); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	}

	if hasRoleUpdate && normalizedRole != targetUser.Role {
		if err := a.store.UpdateUserRole(r.Context(), userID, normalizedRole); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusNotFound, "user not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to update user role")
			return
		}
	}

	if hasPasswordUpdate {
		if err := a.store.UpdateUserPasswordHash(r.Context(), userID, passwordHash); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, http.StatusNotFound, "user not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to update user password")
			return
		}
	}

	updatedUser, err := a.store.GetUserByID(r.Context(), userID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load updated user")
		return
	}

	writeJSON(w, http.StatusOK, updatedUser)
}

func (a *API) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.requireAdmin(w, r); !ok {
		return
	}

	userID, ok := parsePathUserID(w, r)
	if !ok {
		return
	}

	targetUser, err := a.store.GetUserByID(r.Context(), userID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load user")
		return
	}

	if targetUser.Role == store.UserRoleAdmin {
		if err := a.ensureUserIsNotLastAdmin(r.Context(), targetUser); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
	}

	if err := a.store.DeleteUser(r.Context(), userID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete user")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "deleted",
		"id":     userID,
	})
}

func (a *API) handleDashboard(w http.ResponseWriter, r *http.Request) {
	user, ok := a.requireBrowserUser(w, r)
	if !ok {
		return
	}

	notifications, err := a.store.ListNotificationsByUser(r.Context(), user.ID, 50)
	if err != nil {
		http.Error(w, "failed to load notifications", http.StatusInternalServerError)
		return
	}

	if err := pageTemplates.ExecuteTemplate(w, "dashboard", dashboardPageData{
		User:          user,
		Notifications: notifications,
	}); err != nil {
		log.Printf("warn: failed to render dashboard: %v", err)
	}
}

func (a *API) handleLogout(w http.ResponseWriter, r *http.Request) {
	clearSessionCookie(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (a *API) requireUser(w http.ResponseWriter, r *http.Request) (store.User, bool) {
	_ = a.store.DeleteExpiredSessions(context.Background())

	token := bearerToken(r.Header.Get("Authorization"))
	if token == "" {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return store.User{}, false
	}

	user, err := a.store.GetUserBySessionTokenHash(r.Context(), security.HashToken(token))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or expired token")
		return store.User{}, false
	}
	return user, true
}

func (a *API) requireAdmin(w http.ResponseWriter, r *http.Request) (store.User, bool) {
	user, ok := a.requireUser(w, r)
	if !ok {
		return store.User{}, false
	}
	if user.Role != store.UserRoleAdmin {
		writeError(w, http.StatusForbidden, "admin access required")
		return store.User{}, false
	}
	return user, true
}

func (a *API) requireBrowserUser(w http.ResponseWriter, r *http.Request) (store.User, bool) {
	user, ok := a.userFromCookie(r)
	if !ok {
		clearSessionCookie(w)
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return store.User{}, false
	}
	return user, true
}

func (a *API) requireService(w http.ResponseWriter, r *http.Request) (store.ServiceKey, bool) {
	token := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if token == "" {
		token = bearerToken(r.Header.Get("Authorization"))
	}
	if token == "" {
		writeError(w, http.StatusUnauthorized, "missing service api key")
		return store.ServiceKey{}, false
	}

	key, err := a.store.GetServiceKeyByTokenHash(r.Context(), security.HashToken(token))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid service api key")
		return store.ServiceKey{}, false
	}
	return key, true
}

func validateUsername(username string) error {
	username = strings.TrimSpace(username)
	if len(username) < 3 || len(username) > 64 {
		return fmt.Errorf("username length must be between 3 and 64")
	}
	return nil
}

func validatePassword(password string) error {
	if len(password) < 8 || len(password) > 128 {
		return fmt.Errorf("password length must be between 8 and 128")
	}
	return nil
}

func validateUserCredentials(username, password string) error {
	if err := validateUsername(username); err != nil {
		return err
	}
	if err := validatePassword(password); err != nil {
		return err
	}
	return nil
}

func validateRole(role string) (string, error) {
	normalizedRole := store.NormalizeUserRole(role)
	if !store.IsValidUserRole(normalizedRole) {
		return "", fmt.Errorf("role must be admin or user")
	}
	return normalizedRole, nil
}

func validatePushRequest(req pushRequest) error {
	if strings.TrimSpace(req.PushID) == "" {
		return fmt.Errorf("push_id is required")
	}
	if strings.TrimSpace(req.Sender) == "" {
		return fmt.Errorf("sender is required")
	}
	if strings.TrimSpace(req.Title) == "" {
		return fmt.Errorf("title is required")
	}
	if strings.TrimSpace(req.Body) == "" {
		return fmt.Errorf("body is required")
	}
	if len(req.Metadata) > 0 && !json.Valid(req.Metadata) {
		return fmt.Errorf("metadata must be valid json")
	}
	return nil
}

func parsePathUserID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	rawID := strings.TrimSpace(r.PathValue("id"))
	userID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil || userID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid user id")
		return 0, false
	}
	return userID, true
}

func (a *API) ensureUserIsNotLastAdmin(ctx context.Context, user store.User) error {
	if user.Role != store.UserRoleAdmin {
		return nil
	}

	adminCount, err := a.store.CountAdmins(ctx)
	if err != nil {
		return fmt.Errorf("failed to count admins")
	}
	if adminCount <= 1 {
		return fmt.Errorf("cannot modify the last admin")
	}
	return nil
}

var (
	errInvalidInput = errors.New("invalid input")
	errInvalidAuth  = errors.New("invalid auth")
)

type userSession struct {
	Token     string
	ExpiresAt time.Time
	User      store.User
}

type loginPageData struct {
	Error    string
	Username string
}

type dashboardPageData struct {
	User          store.User
	Notifications []store.Notification
}

func (a *API) authenticateAndCreateSession(ctx context.Context, username, password string) (userSession, error) {
	if err := validateUserCredentials(username, password); err != nil {
		return userSession{}, fmt.Errorf("%w: %s", errInvalidInput, err.Error())
	}

	username = strings.TrimSpace(username)
	rec, err := a.store.GetUserByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return userSession{}, fmt.Errorf("%w: invalid username or password", errInvalidAuth)
		}
		return userSession{}, err
	}

	if err := security.CheckPassword(rec.PasswordHash, password); err != nil {
		return userSession{}, fmt.Errorf("%w: invalid username or password", errInvalidAuth)
	}

	token, err := security.GenerateOpaqueToken("ntfus_")
	if err != nil {
		return userSession{}, err
	}

	expiresAt := time.Now().UTC().Add(a.userSessionTTL)
	if err := a.store.CreateSession(ctx, rec.ID, security.HashToken(token), expiresAt); err != nil {
		return userSession{}, err
	}

	return userSession{
		Token:     token,
		ExpiresAt: expiresAt,
		User:      rec.User,
	}, nil
}

func (a *API) userFromCookie(r *http.Request) (store.User, bool) {
	_ = a.store.DeleteExpiredSessions(context.Background())

	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || strings.TrimSpace(cookie.Value) == "" {
		return store.User{}, false
	}

	user, err := a.store.GetUserBySessionTokenHash(r.Context(), security.HashToken(cookie.Value))
	if err != nil {
		return store.User{}, false
	}
	return user, true
}

func (a *API) renderLoginPage(w http.ResponseWriter, status int, data loginPageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := pageTemplates.ExecuteTemplate(w, "login", data); err != nil {
		log.Printf("warn: failed to render login page: %v", err)
	}
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("warn: failed to encode response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func setSessionCookie(w http.ResponseWriter, token string, expiresAt time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  expiresAt,
	})
}

func clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
}

func bearerToken(value string) string {
	parts := strings.Fields(strings.TrimSpace(value))
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		return parts[1]
	}
	return ""
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, sw.status, time.Since(started))
	})
}
