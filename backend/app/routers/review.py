"""Review router for entity deduplication review."""

from fastapi import APIRouter, HTTPException

from app.models.api_models import ApproveRequest, EntityGroup, ReviewResponse

router = APIRouter()

# In-memory storage for review groups (would be database in production)
_review_data: dict[str, list[EntityGroup]] = {}


@router.get("/review/{run_id}", response_model=ReviewResponse)
async def get_review_groups(run_id: str):
    """Get entity groups pending review for a run."""
    if run_id not in _review_data:
        # Return empty review if no groups
        return ReviewResponse(
            run_id=run_id,
            total_groups=0,
            pending_groups=0,
            groups=[],
        )

    groups = _review_data[run_id]
    pending = [g for g in groups if g.status == "pending"]

    return ReviewResponse(
        run_id=run_id,
        total_groups=len(groups),
        pending_groups=len(pending),
        groups=groups,
    )


@router.post("/review/{run_id}/approve")
async def approve_groups(run_id: str, request: ApproveRequest):
    """Approve unification of entity groups."""
    if run_id not in _review_data:
        raise HTTPException(status_code=404, detail="Run not found")

    approved_count = 0
    for group in _review_data[run_id]:
        if group.group_id in request.group_ids:
            group.status = "approved"
            if request.canonical_overrides and group.group_id in request.canonical_overrides:
                group.canonical = request.canonical_overrides[group.group_id]
            approved_count += 1

    return {"approved": approved_count}


@router.post("/review/{run_id}/reject")
async def reject_groups(run_id: str, request: ApproveRequest):
    """Reject unification of entity groups."""
    if run_id not in _review_data:
        raise HTTPException(status_code=404, detail="Run not found")

    rejected_count = 0
    for group in _review_data[run_id]:
        if group.group_id in request.group_ids:
            group.status = "rejected"
            rejected_count += 1

    return {"rejected": rejected_count}


@router.post("/review/{run_id}/approve-all")
async def approve_all(run_id: str, threshold: float = 0.9):
    """Approve all groups above a similarity threshold."""
    if run_id not in _review_data:
        raise HTTPException(status_code=404, detail="Run not found")

    approved_count = 0
    for group in _review_data[run_id]:
        if group.status == "pending" and group.similarity >= threshold:
            group.status = "approved"
            approved_count += 1

    return {"approved": approved_count, "threshold": threshold}
