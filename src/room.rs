use crate::session::{RoomId, SessionId};
use std::collections::{HashMap, HashSet};

/// 게임 룸
///
/// 여러 세션이 모여있는 공간
/// 채팅방, 게임 로비, 게임 방 등으로 사용
#[derive(Debug, Clone)]
pub struct Room {
    /// 룸 ID
    id: RoomId,
    /// 룸 이름
    name: String,
    /// 이 룸에 속한 세션들
    members: HashSet<SessionId>,
    /// 최대 인원 수 (0이면 무제한)
    max_members: usize,
}

impl Room {
    /// 새로운 룸 생성
    pub fn new(id: RoomId, name: String, max_members: usize) -> Self {
        Self {
            id,
            name,
            members: HashSet::new(),
            max_members,
        }
    }

    /// 룸 ID 조회
    pub fn id(&self) -> RoomId {
        self.id
    }

    /// 룸 이름 조회
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 현재 인원 수
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// 최대 인원 수
    pub fn max_members(&self) -> usize {
        self.max_members
    }

    /// 세션이 룸에 입장할 수 있는지 확인
    pub fn can_join(&self, _session_id: SessionId) -> bool {
        if self.max_members > 0 && self.members.len() >= self.max_members {
            return false;
        }
        true
    }

    /// 세션을 룸에 추가
    /// Returns: 성공 여부
    pub fn add_member(&mut self, session_id: SessionId) -> bool {
        if !self.can_join(session_id) {
            return false;
        }
        self.members.insert(session_id)
    }

    /// 세션을 룸에서 제거
    /// Returns: 제거 성공 여부
    pub fn remove_member(&mut self, session_id: SessionId) -> bool {
        self.members.remove(&session_id)
    }

    /// 세션이 룸에 속해있는지 확인
    pub fn has_member(&self, session_id: SessionId) -> bool {
        self.members.contains(&session_id)
    }

    /// 모든 멤버 ID 조회
    pub fn members(&self) -> &HashSet<SessionId> {
        &self.members
    }

    /// 룸이 비어있는지 확인
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// 특정 세션을 제외한 모든 멤버 ID 반환
    pub fn members_except(&self, exclude: SessionId) -> Vec<SessionId> {
        self.members
            .iter()
            .filter(|&&id| id != exclude)
            .copied()
            .collect()
    }
}

/// 룸 매니저
/// 모든 룸을 관리하고 세션-룸 매핑 유
pub struct RoomManager {
    /// RoomId -> Room
    rooms: HashMap<RoomId, Room>,
    /// SessionId -> RoomId (세션이 현재 속한 룸)
    session_to_room: HashMap<SessionId, RoomId>,
    /// 다음 룸 ID
    next_room_id: RoomId,
}

impl RoomManager {
    /// 새로운 룸 매니저 생성
    pub fn new() -> Self {
        Self {
            rooms: HashMap::new(),
            session_to_room: HashMap::new(),
            next_room_id: 1,
        }
    }

    /// 새로운 룸 생성
    ///
    /// # Arguments
    /// * `name` - 룸 이름
    /// * `max_members` - 최대 인원 (0이면 무제한)
    ///
    /// Returns: 생성된 룸의 ID
    pub fn create_room(&mut self, name: String, max_members: usize) -> RoomId {
        let room_id = self.next_room_id;
        self.next_room_id += 1;

        let room = Room::new(room_id, name, max_members);
        self.rooms.insert(room_id, room);

        println!("Room created: id={}, max_members={}", room_id, max_members);
        room_id
    }

    /// 룸 조회
    pub fn get_room(&self, room_id: RoomId) -> Option<&Room> {
        self.rooms.get(&room_id)
    }

    /// 룸 조회 (mutable)
    pub fn get_room_mut(&mut self, room_id: RoomId) -> Option<&mut Room> {
        self.rooms.get_mut(&room_id)
    }

    /// 세션을 룸에 입장시킴
    ///
    /// Returns: 성공 여부
    pub fn join_room(&mut self, session_id: SessionId, room_id: RoomId) -> Result<(), String> {
        // 이미 다른 룸에 있으면 먼저 퇴장
        if let Some(old_room_id) = self.session_to_room.get(&session_id) {
            if *old_room_id != room_id {
                self.leave_room(session_id);
            } else {
                // 이미 같은 룸에 있음
                return Ok(());
            }
        }

        // 룸이 존재하는지 확인
        let room = self
            .rooms
            .get_mut(&room_id)
            .ok_or_else(|| format!("Room {} not found", room_id))?;

        // 입장 가능 여부 확인
        if !room.can_join(session_id) {
            return Err(format!("Room {} is full", room_id));
        }

        // 룸에 추가
        room.add_member(session_id);
        self.session_to_room.insert(session_id, room_id);

        println!(
            "Session {} joined room {} (members: {})",
            session_id,
            room_id,
            room.member_count()
        );

        Ok(())
    }

    /// 세션을 룸에서 퇴장시킴
    ///
    /// Returns: 퇴장한 룸의 ID (없으면 None)
    pub fn leave_room(&mut self, session_id: SessionId) -> Option<RoomId> {
        if let Some(room_id) = self.session_to_room.remove(&session_id) {
            if let Some(room) = self.rooms.get_mut(&room_id) {
                room.remove_member(session_id);

                println!(
                    "Session {} left room {} (remaining: {})",
                    session_id,
                    room_id,
                    room.member_count()
                );

                // 룸이 비었으면 제거 (선택사항)
                if room.is_empty() {
                    self.rooms.remove(&room_id);
                    println!("Room {} removed (empty)", room_id);
                }

                return Some(room_id);
            }
        }

        None
    }

    /// 세션이 현재 속한 룸 ID 조회
    pub fn get_session_room(&self, session_id: SessionId) -> Option<RoomId> {
        self.session_to_room.get(&session_id).copied()
    }

    /// 룸에 있는 모든 세션 ID 조회
    pub fn get_room_members(&self, room_id: RoomId) -> Option<Vec<SessionId>> {
        self.rooms
            .get(&room_id)
            .map(|room| room.members().iter().copied().collect())
    }

    /// 룸에 있는 세션 중 특정 세션을 제외한 모든 세션 ID 조회
    pub fn get_room_members_except(
        &self,
        room_id: RoomId,
        exclude: SessionId,
    ) -> Option<Vec<SessionId>> {
        self.rooms
            .get(&room_id)
            .map(|room| room.members_except(exclude))
    }

    /// 전체 룸 수
    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// 모든 룸 ID 조회
    pub fn all_room_ids(&self) -> Vec<RoomId> {
        self.rooms.keys().copied().collect()
    }
}

impl Default for RoomManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_room_creation() {
        let room = Room::new(1, "Test Room".to_string(), 10);
        assert_eq!(room.id(), 1);
        assert_eq!(room.name(), "Test Room");
        assert_eq!(room.max_members(), 10);
        assert_eq!(room.member_count(), 0);
    }

    #[test]
    fn test_room_join_leave() {
        let mut room = Room::new(1, "Test Room".to_string(), 2);

        assert!(room.add_member(100));
        assert_eq!(room.member_count(), 1);

        assert!(room.add_member(200));
        assert_eq!(room.member_count(), 2);

        // 최대 인원 초과
        assert!(!room.can_join(300));

        // 퇴장
        assert!(room.remove_member(100));
        assert_eq!(room.member_count(), 1);

        // 이제 입장 가능
        assert!(room.can_join(300));
    }

    #[test]
    fn test_room_manager() {
        let mut manager = RoomManager::new();

        // 룸 생성
        let room_id = manager.create_room("Lobby".to_string(), 0);
        assert_eq!(room_id, 1);

        // 세션 입장
        assert!(manager.join_room(100, room_id).is_ok());
        assert!(manager.join_room(200, room_id).is_ok());

        // 세션이 룸에 있는지 확인
        assert_eq!(manager.get_session_room(100), Some(room_id));

        // 룸 멤버 조회
        let members = manager.get_room_members(room_id).unwrap();
        assert_eq!(members.len(), 2);

        // 퇴장
        assert_eq!(manager.leave_room(100), Some(room_id));
        assert_eq!(manager.get_session_room(100), None);
    }

    #[test]
    fn test_room_members_except() {
        let mut room = Room::new(1, "Test".to_string(), 0);
        room.add_member(100);
        room.add_member(200);
        room.add_member(300);

        let members = room.members_except(200);
        assert_eq!(members.len(), 2);
        assert!(members.contains(&100));
        assert!(members.contains(&300));
        assert!(!members.contains(&200));
    }
}
