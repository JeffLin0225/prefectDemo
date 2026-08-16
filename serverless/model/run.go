package model

type RunRequest struct {

	// 加上 binding:"required"，Gin 會在欄位為空時自動報錯
	FlowRunID string `json:"flowRunID" binging:"required"`

}

type RunResponse struct {
	Status string `json:"status"`
	Message string `json:"message"`
}
