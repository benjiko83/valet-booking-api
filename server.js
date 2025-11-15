import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import pkg from 'pg';
const { Pool } = pkg;
import { v4 as uuidv4 } from 'uuid';
import 'express-async-errors';

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// PostgreSQL Connection Pool (Neon Cloud)
// Configured for 10 concurrent users
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:bEvfFhIAhCMFpaWLfXtBdbahynRddkWh@metro.proxy.rlwy.net:51172/railway',
  max: 20,                           // Max pool size (10 users × 2 connections per user)
  idleTimeoutMillis: 30000,          // Close idle connections after 30 seconds
  connectionTimeoutMillis: 5000,     // Timeout if can't get connection in 5 seconds
});

async function getConnection() {
  return pool.connect();
}

// ============= TEST ENDPOINTS =============

app.get('/api/test', (req, res) => {
  res.json({ success: true, message: 'Server is working!' });
});

// ============= HEALTH CHECK =============

app.get('/api/health', async (req, res) => {
  const connection = await getConnection();
  try {
    const settingsResult = await connection.query('SELECT COUNT(*) as count FROM booking_slot_settings');
    const valetsResult = await connection.query('SELECT COUNT(*) as count FROM valets WHERE status = $1', ['active']);
    const rotasResult = await connection.query('SELECT COUNT(*) as count FROM valet_rota WHERE is_active = $1', [true]);
    
    const settings = settingsResult.rows[0];
    const valets = valetsResult.rows[0];
    const rotas = rotasResult.rows[0];
    
    const allConfigured = settings.count > 0 && valets.count > 0 && rotas.count > 0;
    
    res.json({
      success: true,
      status: allConfigured ? 'ready' : 'incomplete',
      checks: {
        booking_slot_settings: { configured: settings.count > 0, count: parseInt(settings.count) },
        valets: { configured: valets.count > 0, count: parseInt(valets.count) },
        valet_rota: { configured: rotas.count > 0, count: parseInt(rotas.count) }
      }
    });
  } finally {
    connection.release();
  }
});

// ============= SETTINGS ENDPOINTS =============

app.get('/api/settings/slot-settings', async (req, res) => {
  const connection = await getConnection();
  try {
    const result = await connection.query('SELECT * FROM booking_slot_settings LIMIT 1');
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'No slot settings configured'
      });
    }
    
    res.json({
      success: true,
      data: result.rows[0]
    });
  } finally {
    connection.release();
  }
});

app.put('/api/settings/slot-settings', async (req, res) => {
  const connection = await getConnection();
  try {
    const {
      slot_start_time,
      slot_end_time,
      slot_duration_minutes,
      break_start_time,
      break_end_time,
      lead_time_hours
    } = req.body;

    if (!slot_start_time || !slot_end_time || !slot_duration_minutes || !break_start_time || !break_end_time || !lead_time_hours) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields'
      });
    }

    const existingResult = await connection.query('SELECT * FROM booking_slot_settings LIMIT 1');

    if (existingResult.rows.length === 0) {
      await connection.query(
        `INSERT INTO booking_slot_settings (slot_start_time, slot_end_time, slot_duration_minutes, break_start_time, break_end_time, lead_time_hours, updated_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [slot_start_time, slot_end_time, slot_duration_minutes, break_start_time, break_end_time, lead_time_hours, 'system']
      );
    } else {
      await connection.query(
        `UPDATE booking_slot_settings SET slot_start_time = $1, slot_end_time = $2, slot_duration_minutes = $3, 
         break_start_time = $4, break_end_time = $5, lead_time_hours = $6, updated_at = NOW() WHERE setting_id = $7`,
        [slot_start_time, slot_end_time, slot_duration_minutes, break_start_time, break_end_time, lead_time_hours, existingResult.rows[0].setting_id]
      );
    }

    res.json({
      success: true,
      message: 'Settings updated successfully'
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

// ============= VALET ROTA ENDPOINTS =============

app.get('/api/settings/valet-rota', async (req, res) => {
  const connection = await getConnection();
  try {
    const result = await connection.query(`
      SELECT 
        vr.*,
        v.name
      FROM valet_rota vr
      LEFT JOIN valets v ON vr.valet_id = v.valet_id
      WHERE vr.is_active = TRUE
      ORDER BY v.name ASC
    `);

    res.json({
      success: true,
      data: result.rows || []
    });
  } catch (error) {
    console.error('Error fetching rotas:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch rotas',
      message: error.message
    });
  } finally {
    connection.release();
  }
});

app.post('/api/settings/valet-rota', async (req, res) => {
  const connection = await getConnection();
  try {
    const {
      rota_id,
      monday_available, monday_capacity,
      tuesday_available, tuesday_capacity,
      wednesday_available, wednesday_capacity,
      thursday_available, thursday_capacity,
      friday_available, friday_capacity,
      saturday_available, saturday_capacity,
      sunday_available, sunday_capacity,
      updated_by
    } = req.body;

    if (!rota_id) {
      return res.status(400).json({
        success: false,
        error: 'Rota ID is required'
      });
    }

    await connection.query(`
      UPDATE valet_rota SET
        monday_available = $1, monday_capacity = $2,
        tuesday_available = $3, tuesday_capacity = $4,
        wednesday_available = $5, wednesday_capacity = $6,
        thursday_available = $7, thursday_capacity = $8,
        friday_available = $9, friday_capacity = $10,
        saturday_available = $11, saturday_capacity = $12,
        sunday_available = $13, sunday_capacity = $14,
        updated_at = NOW(),
        updated_by = $15
      WHERE rota_id = $16
    `, [
      monday_available, monday_capacity,
      tuesday_available, tuesday_capacity,
      wednesday_available, wednesday_capacity,
      thursday_available, thursday_capacity,
      friday_available, friday_capacity,
      saturday_available, saturday_capacity,
      sunday_available, sunday_capacity,
      updated_by || 'admin',
      rota_id
    ]);

    res.json({
      success: true,
      message: 'Rota updated successfully'
    });
  } catch (error) {
    console.error('Error updating rota:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to update rota',
      message: error.message
    });
  } finally {
    connection.release();
  }
});

// ============= VALETS ENDPOINTS =============

app.post('/api/valets', async (req, res) => {
  const connection = await getConnection();
  try {
    const { name, email, phone, status } = req.body;
    
    if (!name) {
      return res.status(400).json({ success: false, error: 'Name is required' });
    }

    const valet_id = uuidv4();
    const rota_id = uuidv4();

    await connection.query(
      'INSERT INTO valets (valet_id, name, email, phone, status) VALUES ($1, $2, $3, $4, $5)',
      [valet_id, name, email || null, phone || null, status || 'active']
    );

    await connection.query(
      `INSERT INTO valet_rota (rota_id, valet_id, monday_available, tuesday_available, wednesday_available, 
       thursday_available, friday_available, saturday_available, sunday_available, is_active, updated_by)
       VALUES ($1, $2, true, true, true, true, true, false, false, true, 'system')`,
      [rota_id, valet_id]
    );

    res.status(201).json({
      success: true,
      message: 'Valet created successfully',
      valet_id
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

app.get('/api/valets', async (req, res) => {
  const connection = await getConnection();
  try {
    const result = await connection.query('SELECT * FROM valets ORDER BY name ASC');
    res.json({
      success: true,
      data: result.rows
    });
  } finally {
    connection.release();
  }
});

app.put('/api/valets/:valet_id', async (req, res) => {
  const connection = await getConnection();
  try {
    const { valet_id } = req.params;
    const { name, email, phone, status } = req.body;

    if (!name) {
      return res.status(400).json({ success: false, error: 'Name is required' });
    }

    await connection.query(
      'UPDATE valets SET name = $1, email = $2, phone = $3, status = $4, updated_at = NOW() WHERE valet_id = $5',
      [name, email || null, phone || null, status || 'active', valet_id]
    );

    res.json({
      success: true,
      message: 'Valet updated successfully'
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

app.delete('/api/valets/:valet_id', async (req, res) => {
  const connection = await getConnection();
  try {
    const { valet_id } = req.params;

    await connection.query('DELETE FROM valet_bookings WHERE valet_id = $1', [valet_id]);
    await connection.query('DELETE FROM valet_holidays WHERE valet_id = $1', [valet_id]);
    await connection.query('DELETE FROM valet_slot_overrides WHERE valet_id = $1', [valet_id]);
    await connection.query('DELETE FROM valet_rota WHERE valet_id = $1', [valet_id]);
    await connection.query('DELETE FROM valets WHERE valet_id = $1', [valet_id]);

    res.json({
      success: true,
      message: 'Valet deleted successfully'
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

// ============= AVAILABILITY ENDPOINTS (KEY!) =============

// BATCH ENDPOINT - Load all dates at once for SPEED! ⚡
app.post('/api/availability/batch', async (req, res) => {
  const connection = await getConnection();
  try {
    const { valet_id, dates } = req.body;
    
    if (!valet_id || !dates || !Array.isArray(dates)) {
      return res.status(400).json({
        success: false,
        error: 'valet_id and dates array required'
      });
    }

    // Get settings once
    const settingsResult = await connection.query('SELECT * FROM booking_slot_settings LIMIT 1');
    if (settingsResult.rows.length === 0) {
      return res.status(500).json({ success: false, error: 'Settings not configured' });
    }
    
    const setting = settingsResult.rows[0];
    const slotDurationMinutes = setting.slot_duration_minutes;
    const startTime = setting.slot_start_time;
    const endTime = setting.slot_end_time;
    const breakStart = setting.break_start_time;
    const breakEnd = setting.break_end_time;
    const leadTimeHours = setting.lead_time_hours;

    // Parse times once
    const [startHour, startMin] = startTime.split(':').map(Number);
    const [endHour, endMin] = endTime.split(':').map(Number);
    const [breakStartHour, breakStartMin] = breakStart.split(':').map(Number);
    const [breakEndHour, breakEndMin] = breakEnd.split(':').map(Number);
    
    const startTotalMin = startHour * 60 + startMin;
    const endTotalMin = endHour * 60 + endMin;
    const breakStartTotalMin = breakStartHour * 60 + breakStartMin;
    const breakEndTotalMin = breakEndHour * 60 + breakEndMin;

    // Get valet
    const valetsResult = await connection.query('SELECT * FROM valets WHERE valet_id = $1', [valet_id]);
    if (valetsResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Valet not found' });
    }

    // Get rota
    const rotasResult = await connection.query('SELECT * FROM valet_rota WHERE valet_id = $1 AND is_active = $2', [valet_id, true]);
    if (rotasResult.rows.length === 0) {
      return res.json({ success: true, valet_id, availability: {}, message: 'No active rota' });
    }

    const rota = rotasResult.rows[0];

    // Get ALL bookings for these dates in ONE query!
    const datesList = dates.map(d => `'${d}'`).join(',');
    const bookingsResult = await connection.query(
      `SELECT DATE(booking_date) as date, booking_time, COUNT(*) as count FROM valet_bookings 
       WHERE valet_id = $1 AND DATE(booking_date) IN (${datesList}) 
       AND status NOT IN ('cancelled', 'completed')
       GROUP BY DATE(booking_date), booking_time`,
      [valet_id]
    );

    // Get holidays for these dates in ONE query!
    const holidaysResult = await connection.query(
      `SELECT holiday_date FROM valet_holidays 
       WHERE valet_id = $1 AND holiday_date IN (${datesList})`,
      [valet_id]
    );

    const holidays = new Set(holidaysResult.rows.map(h => h.holiday_date));
    const bookings = {};
    bookingsResult.rows.forEach(row => {
      const key = `${row.date}:${row.time}`;
      bookings[key] = parseInt(row.count);
    });

    // Process each date
    const availability = {};
    const now = new Date();
    const earliestBookingTime = new Date(now.getTime() + leadTimeHours * 60 * 60 * 1000);

    dates.forEach(dateStr => {
      const dateObj = new Date(dateStr + 'T00:00:00Z');
      
      // Skip past dates
      if (dateObj < new Date(new Date().setDate(new Date().getDate() - 1))) {
        availability[dateStr] = { available: 0, total: 0 };
        return;
      }

      const dayOfWeek = dateObj.getUTCDay();
      const dayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
      const dayName = dayNames[dayOfWeek];
      const capacityKey = `${dayName}_capacity`;
      const availabilityKey = `${dayName}_available`;

      // Check if available on this day
      if (!rota[availabilityKey] || holidays.has(dateStr)) {
        availability[dateStr] = { available: 0, total: 0 };
        return;
      }

      const maxCapacity = rota[capacityKey] || 3;
      let totalAvailable = 0;
      let totalSlots = 0;

      // Generate slots
      for (let currentMin = startTotalMin; currentMin < endTotalMin; currentMin += slotDurationMinutes) {
        if (currentMin >= breakStartTotalMin && currentMin < breakEndTotalMin) continue;

        const slotHour = Math.floor(currentMin / 60);
        const slotMinute = currentMin % 60;
        const slotTime = `${String(slotHour).padStart(2, '0')}:${String(slotMinute).padStart(2, '0')}`;

        const bookingKey = `${dateStr}:${slotTime}`;
        const bookedCount = bookings[bookingKey] || 0;
        const isSlotFull = bookedCount >= maxCapacity;

        totalSlots += 1;
        if (!isSlotFull) {
          totalAvailable += 1;
        }
      }

      availability[dateStr] = { available: totalAvailable, total: totalSlots };
    });

    res.json({
      success: true,
      valet_id,
      availability,
      message: `Batch loaded ${dates.length} dates`
    });

  } catch (error) {
    console.error('Batch error:', error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

app.get('/api/availability/slots/:valet_id/:date', async (req, res) => {
  const connection = await getConnection();
  try {
    const { valet_id, date } = req.params;
    
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ success: false, error: 'Invalid date format' });
    }
    
    const settingsResult = await connection.query('SELECT * FROM booking_slot_settings LIMIT 1');
    if (settingsResult.rows.length === 0) {
      return res.status(500).json({ success: false, error: 'Settings not configured' });
    }
    
    const setting = settingsResult.rows[0];
    const slotDurationMinutes = setting.slot_duration_minutes;
    
    const valetsResult = await connection.query('SELECT * FROM valets WHERE valet_id = $1', [valet_id]);
    if (valetsResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Valet not found' });
    }
    
    const rotasResult = await connection.query('SELECT * FROM valet_rota WHERE valet_id = $1 AND is_active = $2', [valet_id, true]);
    if (rotasResult.rows.length === 0) {
      return res.json({ success: true, date, valet_id, slots: [], message: 'No active rota' });
    }
    
    const rota = rotasResult.rows[0];
    const dateObj = new Date(date + 'T00:00:00Z');
    const dayOfWeek = dateObj.getUTCDay();
    const dayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const dayName = dayNames[dayOfWeek];
    const capacityKey = `${dayName}_capacity`;
    const availabilityKey = `${dayName}_available`;
    
    if (!rota[availabilityKey]) {
      return res.json({ success: true, date, valet_id, slots: [], message: `Not available on ${dayName}` });
    }
    
    const holidaysResult = await connection.query('SELECT * FROM valet_holidays WHERE valet_id = $1 AND holiday_date = $2', [valet_id, date]);
    if (holidaysResult.rows.length > 0) {
      return res.json({ success: true, date, valet_id, slots: [], message: 'Holiday' });
    }
    
    const startTime = setting.slot_start_time;
    const endTime = setting.slot_end_time;
    const breakStart = setting.break_start_time;
    const breakEnd = setting.break_end_time;
    
    const [startHour, startMin] = startTime.split(':').map(Number);
    const [endHour, endMin] = endTime.split(':').map(Number);
    const [breakStartHour, breakStartMin] = breakStart.split(':').map(Number);
    const [breakEndHour, breakEndMin] = breakEnd.split(':').map(Number);
    
    const startTotalMin = startHour * 60 + startMin;
    const endTotalMin = endHour * 60 + endMin;
    const breakStartTotalMin = breakStartHour * 60 + breakStartMin;
    const breakEndTotalMin = breakEndHour * 60 + breakEndMin;
    
    const slots = [];
    const maxCapacity = rota[capacityKey] || 3;
    
    // Get ALL bookings for this date in ONE query! (not per-slot)
    const allBookingsResult = await connection.query(
      `SELECT booking_time, COUNT(*) as count FROM valet_bookings 
       WHERE valet_id = $1 AND DATE(booking_date) = $2 AND status NOT IN ('cancelled', 'completed')
       GROUP BY booking_time`,
      [valet_id, date]
    );
    
    // Build map of booked counts by time
    const bookingsByTime = {};
    allBookingsResult.rows.forEach(row => {
      bookingsByTime[row.booking_time] = parseInt(row.count);
    });
    
    for (let currentMin = startTotalMin; currentMin < endTotalMin; currentMin += slotDurationMinutes) {
      const slotHour = Math.floor(currentMin / 60);
      const slotMinute = currentMin % 60;
      const slotTime = `${String(slotHour).padStart(2, '0')}:${String(slotMinute).padStart(2, '0')}`;
      
      if (currentMin >= breakStartTotalMin && currentMin < breakEndTotalMin) continue;
      
      const bookedCount = bookingsByTime[slotTime] || 0;
      const isSlotFull = bookedCount >= maxCapacity;
      
      slots.push({ 
        time: slotTime, 
        available: !isSlotFull, 
        booked_count: bookedCount,
        capacity: maxCapacity
      });
    }
    
    const availableSlots = slots.filter(s => s.available);
    const now = new Date();
    const leadTimeHours = setting.lead_time_hours;
    const earliestBookingTime = new Date(now.getTime() + leadTimeHours * 60 * 60 * 1000);
    const isToday = date === new Date().toISOString().split('T')[0];
    
    const finalSlots = availableSlots.map(slot => {
      let canBook = slot.available;
      if (isToday) {
        const [slotHour, slotMin] = slot.time.split(':').map(Number);
        const slotDateTime = new Date(now);
        slotDateTime.setHours(slotHour, slotMin, 0);
        canBook = slotDateTime > earliestBookingTime;
      }
      return { ...slot, can_book: canBook };
    });
    
    const dayBookingsResult = await connection.query(
      `SELECT COUNT(*) as total_bookings FROM valet_bookings WHERE valet_id = $1 AND DATE(booking_date) = $2 AND status NOT IN ('cancelled', 'completed')`,
      [valet_id, date]
    );
    
    const totalDayBookings = parseInt(dayBookingsResult.rows[0].total_bookings);
    const bookableSlots = Math.max(0, maxCapacity - totalDayBookings);
    
    res.json({
      success: true,
      date,
      valet_id,
      day_of_week: dayName,
      is_available_today: rota[availabilityKey],
      max_slots_per_day: maxCapacity,
      total_slots_available: slots.length,
      available_slots: finalSlots.length,
      bookable_slots: bookableSlots,
      lead_time_hours: leadTimeHours,
      slots: finalSlots
    });
    
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

// ============= VALET BOOKINGS ENDPOINTS =============

app.get('/api/valet-bookings/grouped/by-date', async (req, res) => {
  const connection = await getConnection();
  try {
    const result = await connection.query(
      `SELECT * FROM valet_bookings 
       WHERE status != $1 
       ORDER BY booking_date ASC, booking_time ASC`,
      ['completed']
    );

    res.json({
      success: true,
      data: result.rows,
      count: result.rows.length
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

app.get('/api/valet-bookings', async (req, res) => {
  const connection = await getConnection();
  try {
    const { filterStatus = 'all', filterSource = 'all', searchTerm = '' } = req.query;
    
    let query = 'SELECT * FROM valet_bookings WHERE 1=1';
    const params = [];
    let paramCount = 1;

    if (filterStatus !== 'all') {
      query += ` AND status = $${paramCount}`;
      params.push(filterStatus);
      paramCount++;
    }

    if (filterSource !== 'all') {
      query += ` AND source = $${paramCount}`;
      params.push(filterSource);
      paramCount++;
    }

    if (searchTerm) {
      query += ` AND (booking_code ILIKE $${paramCount} OR customer_name ILIKE $${paramCount})`;
      params.push(`%${searchTerm}%`);
      paramCount++;
    }
    
    query += ' ORDER BY booking_date DESC LIMIT 100';
    
    const result = await connection.query(query, params);
    
    res.json({
      success: true,
      data: result.rows,
      count: result.rows.length
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to fetch bookings',
      message: error.message
    });
  } finally {
    connection.release();
  }
});

app.delete('/api/valet-bookings/:booking_id', async (req, res) => {
  const connection = await getConnection();
  try {
    const { booking_id } = req.params;
    
    const result = await connection.query(
      'DELETE FROM valet_bookings WHERE booking_id = $1',
      [booking_id]
    );
    
    if (result.rowCount === 0) {
      return res.status(404).json({ success: false, error: 'Booking not found' });
    }
    
    res.json({ 
      success: true, 
      message: 'Booking deleted successfully',
      deleted_id: booking_id
    });
  } catch (error) {
    console.error('Error deleting booking:', error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

app.put('/api/valet-bookings/:booking_id', async (req, res) => {
  const connection = await getConnection();
  try {
    const { booking_id } = req.params;
    
    const result = await connection.query(
      'UPDATE valet_bookings SET status = $1, updated_at = NOW() WHERE booking_id = $2 RETURNING *',
      ['completed', booking_id]
    );
    
    if (result.rowCount === 0) {
      return res.status(404).json({ success: false, error: 'Booking not found' });
    }
    
    res.json({ 
      success: true, 
      message: 'Booking marked as completed',
      booking_id: booking_id,
      status: 'completed'
    });
  } catch (error) {
    console.error('Error updating booking:', error);
    res.status(500).json({ success: false, error: error.message });
  } finally {
    connection.release();
  }
});

app.post('/api/valet-bookings', async (req, res) => {
  const connection = await getConnection();
  try {
    const {
      vehicle_make,
      vehicle_model,
      vehicle_registration,
      vehicle_colour,
      vehicle_condition,
      customer_name,
      customer_email,
      customer_phone,
      booking_date,
      booking_time,
      valet_id,
      valet_name,
      key_number,
      sales_executive_name,
      paint_protection,
      special_requirements,
      notes,
      source,
      prep_tracker_id
    } = req.body;

    if (!vehicle_make || !vehicle_model || !booking_date || !valet_id) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields'
      });
    }

    // ========== MAX CAPACITY VALIDATION START ==========
    // Check if valet has reached max daily capacity
    const rotaResult = await connection.query(
      'SELECT * FROM valet_rota WHERE valet_id = $1 AND is_active = $2',
      [valet_id, true]
    );

    if (rotaResult.rows.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'No active rota found for this valet'
      });
    }

    const rota = rotaResult.rows[0];
    const dateObj = new Date(booking_date + 'T00:00:00Z');
    const dayOfWeek = dateObj.getUTCDay();
    const dayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const dayName = dayNames[dayOfWeek];
    const capacityKey = `${dayName}_capacity`;
    const maxDailyCapacity = rota[capacityKey] || 3;

    // Count existing bookings for this valet on this date
    const bookingCountResult = await connection.query(
      `SELECT COUNT(*) as total_bookings 
       FROM valet_bookings 
       WHERE valet_id = $1 
       AND DATE(booking_date) = $2 
       AND status NOT IN ('cancelled', 'completed')`,
      [valet_id, booking_date]
    );

    const currentBookings = parseInt(bookingCountResult.rows[0].total_bookings);

    // Check if max capacity reached
    if (currentBookings >= maxDailyCapacity) {
      return res.status(409).json({
        success: false,
        error: 'Maximum daily slots reached',
        message: `This valet has reached their maximum capacity of ${maxDailyCapacity} bookings for ${booking_date}. Current bookings: ${currentBookings}.`,
        max_capacity: maxDailyCapacity,
        current_bookings: currentBookings
      });
    }
    // ========== MAX CAPACITY VALIDATION END ==========

    const booking_id = uuidv4();
    const booking_code = `VB-${Date.now()}`;

    await connection.query(
      `INSERT INTO valet_bookings (
        booking_id, booking_code, vehicle_make, vehicle_model, vehicle_registration,
        vehicle_colour, vehicle_condition, customer_name, customer_email, customer_phone,
        booking_date, booking_time, valet_id, valet_name, status, paint_protection,
        special_requirements, notes, key_number, sales_executive_name, source, prep_tracker_id
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`,
      [
        booking_id, booking_code, vehicle_make, vehicle_model, vehicle_registration,
        vehicle_colour || null, vehicle_condition || 'used', customer_name || null,
        customer_email || null, customer_phone || null, booking_date, booking_time || null,
        valet_id, valet_name || null, 'pending', paint_protection || 'no',
        special_requirements || null, notes || null, key_number || null, sales_executive_name || null, source || 'manual', prep_tracker_id || null
      ]
    );

    res.status(201).json({
      success: true,
      message: 'Booking created successfully',
      booking_id,
      booking_code
    });
  } catch (error) {
    console.error('Error creating booking:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to create booking',
      message: error.message
    });
  } finally {
    connection.release();
  }
});

// ============= START SERVER =============

app.listen(PORT, () => {
  console.log(`✅ Valet Booking API running on http://localhost:${PORT}`);
  console.log(`📝 Connected to PostgreSQL database: neondb`);
});

export default app;
